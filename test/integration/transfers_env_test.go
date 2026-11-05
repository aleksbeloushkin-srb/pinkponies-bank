//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/fakerisk"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/grpcapi"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/ledgerclient"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/longpoll"
	transfersmetrics "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/metrics"
	transferspg "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/riskclient"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// errCrash is the panic value of a simulated process crash.
var errCrash = errors.New("simulated crash")

// sagaEnv is the transfers service on its database, talking to a real
// ledger (on its own database, over gRPC on a local TCP port) and to the
// fake screening provider over HTTP. One clock drives all three.
type sagaEnv struct {
	clock    *clock
	ledger   *ledgerEnv
	risk     *fakerisk.Server
	riskURL  string
	tx       *crashingTx
	store    *transferspg.Transfers
	tasks    *taskqueue.Postgres
	outbox   *transferspg.Outbox
	waiters  *longpoll.Registry
	ledgerC  app.Ledger
	svc      *app.Service
	worker   *taskqueue.Worker
	src, dst uuid.UUID
}

type sagaOptions struct {
	riskTimeout time.Duration
	wrapLedger  func(app.Ledger) app.Ledger
	riskFaults  []string
}

func newSagaEnv(t *testing.T, opts sagaOptions) *sagaEnv {
	t.Helper()
	if opts.riskTimeout == 0 {
		opts.riskTimeout = time.Second
	}
	if _, err := transfersDB.Exec(context.Background(), `TRUNCATE transfers, saga_steps, tasks, outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	e := &sagaEnv{ledger: newLedgerEnv(t)}
	e.clock = e.ledger.clock
	e.src = e.ledger.open(t, "Clover Hayfield", 10_000).ID
	e.dst = e.ledger.open(t, "Pip Sugarmane", 0).ID

	// The ledger's real gRPC server on a local port.
	srv, _ := grpcapi.NewServer(grpcapi.NewHandlers(e.ledger.svc, discard()), grpcapi.NewMetrics(prometheus.NewRegistry()), discard())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = srv.Serve(lis)
	}()
	conn, err := ledgerclient.Dial("passthrough:///"+lis.Addr().String(), ledgerclient.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}

	// The fake provider over HTTP.
	e.risk = fakerisk.New(fakerisk.Config{ReviewDelay: 10 * time.Second, RetryAfter: time.Second, Faults: opts.riskFaults}, e.clock, discard())
	riskSrv := httptest.NewServer(e.risk.Handler())
	e.riskURL = riskSrv.URL
	screener, err := riskclient.New(riskclient.Config{BaseURL: riskSrv.URL, CallTimeout: opts.riskTimeout}, riskSrv.Client(), riskclient.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.risk.Close()
		riskSrv.Close()
		_ = conn.Close()
		srv.Stop()
		<-served
	})

	e.ledgerC = ledgerclient.New(conn, ledgerclient.Config{CallTimeout: 2 * time.Second})
	if opts.wrapLedger != nil {
		e.ledgerC = opts.wrapLedger(e.ledgerC)
	}
	e.tx = &crashingTx{inner: txctx.New(transfersDB)}
	e.store = transferspg.NewTransfers(txctx.New(transfersDB))
	e.tasks = taskqueue.NewPostgres(txctx.New(transfersDB))
	e.outbox = transferspg.NewOutbox(txctx.New(transfersDB))
	e.waiters = longpoll.NewRegistry(100, longpoll.NewMetrics(prometheus.NewRegistry()))
	e.svc = app.NewService(app.Deps{
		Tx: e.tx, Transfers: e.store, Tasks: e.tasks, Events: e.outbox,
		Ledger: e.ledgerC, Screener: screener, Watcher: e.waiters, Clock: e.clock,
		Metrics: transfersmetrics.NewBusiness(prometheus.NewRegistry()), NewID: uuid.NewV7, Logger: discard(),
	}, app.Config{
		HoldTTL: 15 * time.Minute, ScreenMaxFailures: 4, ScreenReviewTimeout: 2 * time.Minute,
		ScreenPollMin: time.Second, ScreenPollMax: 30 * time.Second,
		WaitTimeout: 5 * time.Second, WaitReadTimeout: 2 * time.Second,
	})
	e.worker = taskqueue.NewWorker(e.tasks, e.svc, e.clock, nil, taskqueue.NewMetrics(prometheus.NewRegistry(), app.TaskKinds), discard(),
		taskqueue.Config{
			Concurrency: 4, IdleInterval: time.Hour, Lease: time.Minute, TaskTimeout: 10 * time.Second, MaxAttempts: 10,
			Backoff: taskqueue.Backoff{Base: 100 * time.Millisecond, Max: time.Second, Rand: func(int64) int64 { return 0 }},
		})
	return e
}

// create accepts a transfer, retrying like a client after a crash: with the
// same Idempotency-Key until there is an answer.
func (e *sagaEnv) create(t *testing.T, amount int64) uuid.UUID {
	t.Helper()
	for range 5 {
		res, err := func() (res app.CreateTransferResult, err error) {
			defer func() {
				if p := recover(); p != nil {
					err = errCrash
				}
			}()
			return e.svc.CreateTransfer(context.Background(), app.CreateTransferRequest{
				IdempotencyKey: "client-key", SourceAccountID: e.src, DestinationAccountID: e.dst,
				AmountMinor: amount, Currency: "EUR",
			})
		}()
		if err == nil {
			return res.Transfer.ID
		}
	}
	t.Fatal("create never succeeded")
	return uuid.Nil
}

// drain runs the worker until no task is ready, moving the clock to the
// next due task whenever nothing is due now.
func (e *sagaEnv) drain(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for range 500 {
		n, err := e.worker.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			continue
		}
		var due *time.Time
		if err := transfersDB.QueryRow(ctx, `SELECT min(run_after) FROM tasks WHERE state = 'ready'`).Scan(&due); err != nil {
			t.Fatal(err)
		}
		if due == nil {
			return
		}
		if due.After(e.clock.Now()) {
			e.clock.mu.Lock()
			e.clock.now = *due
			e.clock.mu.Unlock()
		}
	}
	t.Fatal("the saga did not settle in 500 rounds")
}

func (e *sagaEnv) transfer(t *testing.T, id uuid.UUID) transfers.Transfer {
	t.Helper()
	tr, err := e.store.Transfer(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// crashingTx wraps the transaction manager: the transaction numbered
// before is rolled back and then the process "dies"; the one numbered
// after commits and then the process dies.
type crashingTx struct {
	inner         *txctx.Manager
	mu            sync.Mutex
	runs          int
	before, after int
}

func (c *crashingTx) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	c.mu.Lock()
	c.runs++
	n := c.runs
	c.mu.Unlock()
	err := c.inner.Run(ctx, func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return err
		}
		if n == c.before {
			return errCrash // rolls the transaction back
		}
		return nil
	})
	if n == c.before || (err == nil && n == c.after) {
		panic(errCrash)
	}
	return err
}

func (c *crashingTx) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

// crashingLedger runs each call for real and then "dies" after call
// number at, or loses its answer.
type crashingLedger struct {
	app.Ledger
	mu    sync.Mutex
	calls []string
	at    int
	lose  bool
}

func (c *crashingLedger) after(name string) error {
	c.mu.Lock()
	c.calls = append(c.calls, name)
	hit := len(c.calls) == c.at
	c.mu.Unlock()
	if !hit {
		return nil
	}
	if c.lose {
		return app.ErrLedgerUnavailable
	}
	panic(errCrash)
}

func (c *crashingLedger) PlaceHold(ctx context.Context, req app.HoldRequest) (app.Hold, error) {
	h, err := c.Ledger.PlaceHold(ctx, req)
	if err == nil {
		err = c.after("place")
	}
	return h, err
}

func (c *crashingLedger) CaptureHold(ctx context.Context, id uuid.UUID, key string) (app.Capture, error) {
	r, err := c.Ledger.CaptureHold(ctx, id, key)
	if err == nil {
		err = c.after("capture")
	}
	return r, err
}

func (c *crashingLedger) ReleaseHold(ctx context.Context, id uuid.UUID, key, reason string) (app.Release, error) {
	r, err := c.Ledger.ReleaseHold(ctx, id, key, reason)
	if err == nil {
		err = c.after("release")
	}
	return r, err
}
