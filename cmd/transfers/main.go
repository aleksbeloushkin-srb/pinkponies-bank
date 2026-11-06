// Command transfers is the Pinkponies Bank transfers service: the REST API
// clients use, and the orchestrator of the transfer saga (hold, screen,
// capture or release) over the ledger's gRPC API and the screening
// provider. TRANSFERS_ROLES picks which parts run in this process.
//
// main is the composition root. It is the only place that knows every
// concrete type: it reads the configuration, builds the adapters, injects
// them into the use cases, starts the components and stops them in the
// right order. Read it top to bottom.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/kafka"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/httpapi"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/kafkapub"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/ledgerclient"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/longpoll"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/metrics"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/migrations"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/outbox"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/riskclient"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// metricsQueryTimeout bounds the database queries behind the gauges, well
// below the default Prometheus scrape timeout of 10 s.
const metricsQueryTimeout = 2 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv(envPrefix + "ADMIN_ADDR")
		if addr == "" {
			addr = ":9100"
		}
		os.Exit(platform.Healthcheck(addr))
	}

	// Catch signals before anything else, so a SIGTERM during start-up
	// still goes through the orderly path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig(os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, "transfers: config:", err)
		os.Exit(1)
	}
	log := platform.NewLogger(os.Stdout, cfg.LogLevel)

	if err := run(ctx, stop, cfg, log); err != nil {
		log.Error("transfers stopped with an error", "err", err)
		os.Exit(1)
	}
	log.Info("transfers stopped")
}

// run builds the service, serves until ctx is cancelled or a component
// fails, and then shuts down in order:
//
//  1. readiness turns false, so the load balancer stops sending traffic,
//     and the worker stops claiming new steps (running ones continue);
//  2. a short pause lets the readiness change propagate;
//  3. the API server drains in-flight requests (long polls are released
//     at once by closing the waiter registry);
//  4. background components stop: the worker finishes its running steps,
//     the relay its current batch;
//  5. the admin listener (metrics, probes) stops;
//  6. the Kafka client closes (the relay produces synchronously, so nothing
//     is left in its buffers), then the ledger connection and the database
//     pool close (deferred, in that order).
func run(ctx context.Context, stopSignals context.CancelFunc, cfg config, log *slog.Logger) error {
	pool, err := pgdb.Open(ctx, pgdb.Config{
		URL:              cfg.DatabaseURL,
		ApplicationName:  "transfers",
		MaxConns:         int32(cfg.DBMaxConns),
		StatementTimeout: cfg.DBStatementTimeout,
		LockTimeout:      cfg.DBLockTimeout,
		IdleInTxTimeout:  cfg.DBIdleInTxTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	results, err := pgdb.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		return err
	}
	for _, r := range results {
		log.Info("migration applied", "version", r.Source.Version, "path", r.Source.Path)
	}

	// One registry, owned here and injected; nothing uses the global one.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		pgdb.NewPoolCollector(pool),
		taskqueue.NewCollector(pool, app.TaskKinds, metricsQueryTimeout, log),
		postgres.NewCollector(pool, metricsQueryTimeout, log),
	)

	// Adapters.
	clock := platform.SystemClock{}
	tx := txctx.New(pool)
	store := postgres.NewTransfers(tx)
	tasks := taskqueue.NewPostgres(tx)
	events := postgres.NewOutbox(tx)

	conn, err := ledgerclient.Dial(cfg.LedgerAddr, ledgerclient.NewMetrics(reg))
	if err != nil {
		return fmt.Errorf("ledger client: %w", err)
	}
	defer conn.Close()
	ledger := ledgerclient.New(conn, ledgerclient.Config{CallTimeout: cfg.LedgerTimeout})

	screener, err := newScreener(cfg, riskclient.NewMetrics(reg))
	if err != nil {
		return err
	}
	longpollMetrics := longpoll.NewMetrics(reg)
	waiters := longpoll.NewRegistry(cfg.WaitMaxWaiters, longpollMetrics)

	// Use cases.
	svc := app.NewService(app.Deps{
		Tx:        tx,
		Transfers: store,
		Tasks:     tasks,
		Events:    events,
		Ledger:    ledger,
		Screener:  screener,
		Watcher:   waiters,
		Clock:     clock,
		Metrics:   metrics.NewBusiness(reg),
		NewID:     uuid.NewV7,
		Logger:    log,
	}, app.Config{
		HoldTTL:             cfg.HoldTTL,
		ScreenMaxFailures:   cfg.ScreenMaxFailures,
		ScreenReviewTimeout: cfg.ScreenReviewTimeout,
		ScreenPollMin:       cfg.ScreenPollMin,
		ScreenPollMax:       cfg.ScreenPollMax,
		WaitTimeout:         cfg.WaitTimeout,
		WaitReadTimeout:     cfg.WaitReadTimeout,
	})

	// Background components run on their own context: they must keep going
	// while HTTP drains.
	bg := platform.NewGroup(log)
	if cfg.Roles.API {
		batcher := longpoll.NewBatcher(waiters, store, longpollMetrics, longpoll.BatcherConfig{
			Interval:     cfg.WaitBatchInterval,
			QueryTimeout: cfg.WaitReadTimeout,
		}, log)
		bg.Go("longpoll-batcher", batcher.Run)
	}
	stopClaiming := func() {}
	if cfg.Roles.Worker {
		w := taskqueue.NewWorker(tasks, svc, clock, tasks.Ready(), taskqueue.NewMetrics(reg, app.TaskKinds), log, taskqueue.Config{
			Concurrency:  cfg.WorkerConcurrency,
			IdleInterval: cfg.WorkerIdleInterval,
			Lease:        cfg.TaskLease,
			TaskTimeout:  cfg.TaskTimeout,
			MaxAttempts:  cfg.TaskMaxAttempts,
			Backoff:      taskqueue.Backoff{Base: cfg.TaskRetryBase, Max: cfg.TaskRetryMax},
		})
		bg.Go("saga-worker", w.Run)
		stopClaiming = w.StopClaiming
	}
	if cfg.Roles.Relay {
		// The relay does not wait for Kafka at start: the outbox absorbs a
		// broker outage, and the relay catches up once it is back.
		cl, err := kafka.NewClient(kafka.Config{Brokers: cfg.KafkaBrokers, ClientID: "transfers-relay"}, log,
			kafkapub.ProducerOptions(cfg.PublishTimeout)...)
		if err != nil {
			return errors.Join(err, bg.Stop())
		}
		defer cl.Close()
		relay := outbox.NewRelay(events, kafkapub.New(cl, cfg.KafkaTopic), clock, events.Ready(), outbox.NewMetrics(reg), outbox.Config{
			Interval:       cfg.RelayInterval,
			Batch:          cfg.RelayBatch,
			Lease:          cfg.RelayLease,
			PublishTimeout: cfg.PublishTimeout,
		}, log)
		bg.Go("outbox-relay", relay.Run)
	}

	// HTTP. The admin listener runs in every role; the API only with the
	// api role. Both listen synchronously, so a busy port fails the start.
	var ready platform.Readiness
	admin, err := platform.StartServer("admin", httpserver.NewServer(httpserver.AdminServerConfig(cfg.AdminAddr),
		httpserver.NewAdminHandler(reg, ready.Ready, log), log))
	if err != nil {
		return errors.Join(err, bg.Stop())
	}
	var api *platform.Server
	if cfg.Roles.API {
		srv := httpserver.NewServer(httpserver.ServerConfig{
			Addr:              cfg.HTTPAddr,
			ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       cfg.HTTPIdleTimeout,
		}, httpapi.NewHandler(httpapi.Deps{
			Creator:          svc,
			Reader:           svc,
			Accounts:         svc,
			Static:           staticUI(),
			NotificationsURL: cfg.UINotificationsURL,
			Metrics:          httpserver.NewMetrics(reg, httpapi.WaitPattern),
			Logger:           log,
		}), log)
		srv.RegisterOnShutdown(waiters.Close)
		if api, err = platform.StartServer("api", srv); err != nil {
			return errors.Join(err, admin.Stop(ctx, httpserver.AdminShutdownTimeout), bg.Stop())
		}
	}
	ready.Set(true)
	log.Info("transfers started", "roles", cfg.Roles, "addr", cfg.HTTPAddr, "admin_addr", cfg.AdminAddr)

	var apiDone <-chan struct{} // nil without the api role: never ready
	if api != nil {
		apiDone = api.Done()
	}
	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "cause", context.Cause(ctx))
	case <-apiDone:
		runErr = fmt.Errorf("api server stopped: %w", api.Err())
	case <-admin.Done():
		runErr = fmt.Errorf("admin server stopped: %w", admin.Err())
	case <-bg.Done():
		runErr = errors.New("a background component stopped")
	}
	// After the first signal the default handler is back, so a second
	// signal kills the process at once.
	stopSignals()

	var errs []error
	ready.Set(false)
	// No new steps from here on; running ones finish while HTTP drains.
	stopClaiming()
	if api != nil {
		log.Info("draining: readiness is off", "pause", cfg.ShutdownDrainDelay.String())
		pause := time.NewTimer(cfg.ShutdownDrainDelay)
		<-pause.C
		if err := api.Stop(ctx, cfg.ShutdownTimeout); err != nil {
			errs = append(errs, err)
		}
		log.Info("api server stopped")
	}
	if err := bg.Stop(); err != nil {
		errs = append(errs, err)
	}
	log.Info("background components stopped")
	// The admin listener goes last: probes and metrics stay available while
	// everything else drains.
	if err := admin.Stop(ctx, httpserver.AdminShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(runErr, errors.Join(errs...))
}

// newScreener builds the screening client. Only the worker role screens, so
// a process without it may run without a provider URL.
func newScreener(cfg config, m *riskclient.Metrics) (app.Screener, error) {
	if !cfg.Roles.Worker && cfg.RiskURL == "" {
		return noScreener{}, nil
	}
	c, err := riskclient.New(riskclient.Config{BaseURL: cfg.RiskURL, CallTimeout: cfg.RiskTimeout, MaxRetryAfter: cfg.RiskMaxRetryAfter}, platform.NewHTTPClient(), m)
	if err != nil {
		// A nil interface, not an interface holding a nil *Client.
		return nil, err
	}
	return c, nil
}

// noScreener stands in for the provider in processes without the worker
// role. Saga steps, its only callers, never run there.
type noScreener struct{}

func (noScreener) Screen(context.Context, app.ScreeningRequest) (app.Screening, error) {
	return app.Screening{}, fmt.Errorf("%w: this process has no worker role", app.ErrScreeningUnavailable)
}
