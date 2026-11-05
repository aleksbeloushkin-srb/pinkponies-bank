//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/fakerisk"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

// ledgerFacts reads, from the ledger's own tables, what happened to the
// money of one transfer.
type ledgerFacts struct {
	holds, transfers   int64 // holds placed, transfer transactions
	holdStatus         string
	srcPosted, srcHeld int64
	dstPosted          int64
}

func (e *sagaEnv) facts(t *testing.T, id uuid.UUID) ledgerFacts {
	t.Helper()
	ctx := context.Background()
	var f ledgerFacts
	key := transfers.StepKey(id, transfers.StepHold)
	f.holds = count(t, ledgerDB, `SELECT count(*) FROM holds WHERE idempotency_key = $1`, key)
	f.transfers = count(t, ledgerDB, `SELECT count(*) FROM transactions t JOIN holds h ON h.id = t.hold_id WHERE h.idempotency_key = $1`, key)
	_ = ledgerDB.QueryRow(ctx, `SELECT status FROM holds WHERE idempotency_key = $1`, key).Scan(&f.holdStatus)
	if err := ledgerDB.QueryRow(ctx, `SELECT posted_minor, held_minor FROM accounts WHERE id = $1`, e.src).Scan(&f.srcPosted, &f.srcHeld); err != nil {
		t.Fatal(err)
	}
	if err := ledgerDB.QueryRow(ctx, `SELECT posted_minor FROM accounts WHERE id = $1`, e.dst).Scan(&f.dstPosted); err != nil {
		t.Fatal(err)
	}
	return f
}

func (e *sagaEnv) eventTypes(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	rows, err := transfersDB.Query(context.Background(), `SELECT type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestSagaHappyPath(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	id := e.create(t, 1250)
	e.drain(t)

	tr := e.transfer(t, id)
	if tr.Status != transfers.StatusCompleted || len(tr.Steps) != 3 {
		t.Fatalf("transfer = %s with %d steps", tr.Status, len(tr.Steps))
	}
	f := e.facts(t, id)
	if f.holds != 1 || f.transfers != 1 || f.holdStatus != "captured" || f.srcPosted != 8_750 || f.srcHeld != 0 || f.dstPosted != 1_250 {
		t.Fatalf("ledger = %+v", f)
	}
	if got := e.eventTypes(t, id); !slices.Equal(got, []string{"transfer.created", "transfer.held", "transfer.completed"}) {
		t.Fatalf("outbox = %v", got)
	}
	if n := count(t, transfersDB, `SELECT count(*) FROM tasks WHERE state <> 'done'`); n != 0 {
		t.Fatalf("%d tasks not done", n)
	}
	checkLedgerBooks(t, 10_000)
}

func TestSagaRefusalsAndCompensation(t *testing.T) {
	tests := []struct {
		name       string
		amount     int64
		status     transfers.Status
		reason     transfers.Reason
		holdStatus string
	}{
		{"insufficient funds", 10_001, transfers.StatusRejected, transfers.ReasonInsufficientFunds, ""},
		{"declined, then released", 1266, transfers.StatusRejected, transfers.ReasonScreeningDeclined, "released"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newSagaEnv(t, sagaOptions{})
			id := e.create(t, tt.amount)
			e.drain(t)
			if tr := e.transfer(t, id); tr.Status != tt.status || tr.Reason != tt.reason {
				t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
			}
			f := e.facts(t, id)
			if f.holdStatus != tt.holdStatus || f.transfers != 0 || f.srcPosted != 10_000 || f.srcHeld != 0 {
				t.Fatalf("ledger = %+v", f)
			}
			checkLedgerBooks(t, 10_000)
		})
	}
}

// The provider executes the screening and never answers; the saga times
// out, retries with the same key, gets the stored decision and completes.
// The provider screened once, the ledger captured once.
func TestSagaScreeningHangThenSameKeyRetry(t *testing.T) {
	// The hung call times out after a second; the retries answer at once.
	e := newSagaEnv(t, sagaOptions{riskTimeout: time.Second, riskFaults: []string{fakerisk.FaultHang}})
	id := e.create(t, 1250)
	e.drain(t)

	if tr := e.transfer(t, id); tr.Status != transfers.StatusCompleted {
		t.Fatalf("status = %s", tr.Status)
	}
	// At least two calls: the hung one and its retry (a slow machine may
	// time out more of them); always one execution.
	calls, executions := e.screening(t, id)
	if calls < 2 || executions != 1 {
		t.Fatalf("screening calls %d, executions %d; want 2 or more and 1", calls, executions)
	}
	if f := e.facts(t, id); f.transfers != 1 || f.srcPosted != 8_750 {
		t.Fatalf("ledger = %+v", f)
	}
}

func (e *sagaEnv) screening(t *testing.T, id uuid.UUID) (calls, executions int) {
	t.Helper()
	resp, err := http.Get(e.riskURL + "/_control/screening?key=" + transfers.StepKey(id, transfers.StepScreen))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sc struct {
		Calls int `json:"calls"`
	}
	if err := jsonDecode(resp, &sc); err != nil {
		t.Fatal(err)
	}
	ctl, err := http.Get(e.riskURL + "/_control")
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.Body.Close()
	var c struct {
		Stats map[string]int `json:"stats"`
	}
	if err := jsonDecode(ctl, &c); err != nil {
		t.Fatal(err)
	}
	return sc.Calls, c.Stats["executions"]
}

func TestSagaReviewThenApprove(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	id := e.create(t, 1277)
	e.drain(t)
	tr := e.transfer(t, id)
	if tr.Status != transfers.StatusCompleted {
		t.Fatalf("status = %s", tr.Status)
	}
	if rec, _ := tr.Step(transfers.StepScreen); rec.Outcome != transfers.OutcomeApprove || rec.Attempts < 2 {
		t.Fatalf("screen step = %+v", rec)
	}
}

// TestSagaCrashPoints is the PostgreSQL and gRPC version of the unit
// test of every crash point: the saga crashes before or after each commit
// and after each ledger call (dying, or losing the answer), runs again,
// and the ledger's tables must show one hold and exactly one capture or one
// release.
func TestSagaCrashPoints(t *testing.T) {
	paths := []struct {
		name      string
		amount    int64
		status    transfers.Status
		captured  int64
		holdState string
	}{
		{"approve", 1250, transfers.StatusCompleted, 1, "captured"},
		{"decline", 1266, transfers.StatusRejected, 0, "released"},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			clean := newSagaEnv(t, sagaOptions{wrapLedger: func(l app.Ledger) app.Ledger {
				return &crashingLedger{Ledger: l}
			}})
			clean.create(t, path.amount)
			clean.drain(t)
			txs := clean.tx.count()
			var ledgerCalls int
			if cl, ok := clean.ledgerC.(*crashingLedger); ok {
				ledgerCalls = len(cl.calls)
			}

			check := func(t *testing.T, e *sagaEnv, id uuid.UUID) {
				t.Helper()
				if tr := e.transfer(t, id); tr.Status != path.status {
					t.Fatalf("status = %s, want %s", tr.Status, path.status)
				}
				f := e.facts(t, id)
				if f.holds != 1 || f.transfers != path.captured || f.holdStatus != path.holdState || f.srcHeld != 0 ||
					f.srcPosted != 10_000-path.captured*path.amount {
					t.Fatalf("ledger = %+v", f)
				}
				if n := count(t, transfersDB, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, id); n != 3 {
					t.Fatalf("outbox events = %d, want 3", n)
				}
				checkLedgerBooks(t, 10_000)
			}
			for k := 1; k <= txs; k++ {
				for _, after := range []bool{false, true} {
					name := fmt.Sprintf("before commit of tx %d", k)
					if after {
						name = fmt.Sprintf("after commit of tx %d", k)
					}
					t.Run(name, func(t *testing.T) {
						e := newSagaEnv(t, sagaOptions{})
						if after {
							e.tx.after = k
						} else {
							e.tx.before = k
						}
						id := e.create(t, path.amount)
						e.drain(t)
						check(t, e, id)
					})
				}
			}
			for k := 1; k <= ledgerCalls; k++ {
				for _, lose := range []bool{false, true} {
					t.Run(fmt.Sprintf("ledger call %d, lost answer %v", k, lose), func(t *testing.T) {
						e := newSagaEnv(t, sagaOptions{wrapLedger: func(l app.Ledger) app.Ledger {
							return &crashingLedger{Ledger: l, at: k, lose: lose}
						}})
						id := e.create(t, path.amount)
						e.drain(t)
						check(t, e, id)
					})
				}
			}
		})
	}
}

// The ledger client and the ledger server agree on the error contract.
func TestLedgerContractOverGRPC(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	ctx := context.Background()
	req := app.HoldRequest{Key: "contract:hold", SourceID: e.src, DestinationID: e.dst, AmountMinor: 100, Currency: "EUR", TTL: time.Minute}

	first, err := e.ledgerC.PlaceHold(ctx, req)
	if err != nil || first.Replayed {
		t.Fatalf("first hold = %+v, %v", first, err)
	}
	again, err := e.ledgerC.PlaceHold(ctx, req)
	if err != nil || !again.Replayed || again.ID != first.ID {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	big := req
	big.Key, big.AmountMinor = "contract:big", 1_000_000
	if _, err := e.ledgerC.PlaceHold(ctx, big); !errorsIs(err, app.ErrInsufficientFunds) {
		t.Fatalf("overdraft: %v", err)
	}
	other := req
	other.Key, other.DestinationID = "contract:ghost", uuid.New()
	if _, err := e.ledgerC.PlaceHold(ctx, other); !errorsIs(err, app.ErrAccountNotFound) {
		t.Fatalf("unknown account: %v", err)
	}
	reused := req
	reused.AmountMinor = 101
	if _, err := e.ledgerC.PlaceHold(ctx, reused); !errorsIs(err, app.ErrLedgerContract) {
		t.Fatalf("key reused: %v", err)
	}

	c, err := e.ledgerC.CaptureHold(ctx, first.ID, "contract:capture")
	if err != nil || c.Replayed {
		t.Fatalf("capture = %+v, %v", c, err)
	}
	if c2, err := e.ledgerC.CaptureHold(ctx, first.ID, "contract:capture"); err != nil || !c2.Replayed || c2.TransactionID != c.TransactionID {
		t.Fatalf("capture replay = %+v, %v", c2, err)
	}
	if _, err := e.ledgerC.ReleaseHold(ctx, first.ID, "contract:release", "late"); !errorsIs(err, app.ErrHoldCaptured) {
		t.Fatalf("release after capture: %v", err)
	}

	expiring := req
	expiring.Key = "contract:expiring"
	h, err := e.ledgerC.PlaceHold(ctx, expiring)
	if err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Minute)
	if _, err := e.ledgerC.CaptureHold(ctx, h.ID, "contract:late-capture"); !errorsIs(err, app.ErrHoldExpired) {
		t.Fatalf("capture after expiry: %v", err)
	}
	if r, err := e.ledgerC.ReleaseHold(ctx, h.ID, "contract:release2", "expired"); err != nil || r.Replayed {
		t.Fatalf("release of an expired hold = %+v, %v", r, err)
	}
	if r, err := e.ledgerC.ReleaseHold(ctx, h.ID, "contract:release3", "again"); err != nil || !r.Replayed {
		t.Fatalf("second release = %+v, %v", r, err)
	}
	accounts, err := e.ledgerC.Accounts(ctx)
	if err != nil || len(accounts) != 2 {
		t.Fatalf("accounts = %+v, %v (the treasury must be filtered out)", accounts, err)
	}
}

// A long poll is woken by the batcher when the saga moves the transfer.
func TestLongPollFollowsTheSaga(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	id := e.create(t, 1250)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcherDone := make(chan error)
	go func() {
		batcherDone <- newBatcher(e).Run(ctx)
	}()

	type result struct {
		res app.WaitResult
		err error
	}
	waited := make(chan result)
	go func() {
		res, err := e.svc.WaitTransfer(context.Background(), id, 1)
		waited <- result{res, err}
	}()
	if _, err := e.worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := <-waited
	if r.err != nil || !r.res.Modified || r.res.Transfer.Version <= 1 {
		t.Fatalf("WaitTransfer = %+v, %v", r.res, r.err)
	}
	cancel()
	if err := <-batcherDone; err != nil {
		t.Fatal(err)
	}
}
