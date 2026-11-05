//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	ledgerapp "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
	ledgermetrics "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/metrics"
	ledgerpg "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// ledgerEnv is a ledger service on the shared ledger database.
type ledgerEnv struct {
	store *ledgerpg.Store
	clock *clock
	svc   *ledgerapp.Service
}

func newLedgerEnv(t *testing.T) *ledgerEnv {
	t.Helper()
	if _, err := ledgerDB.Exec(context.Background(),
		`TRUNCATE entries, transactions, holds, accounts RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	tx := txctx.New(ledgerDB)
	e := &ledgerEnv{store: ledgerpg.NewStore(tx), clock: newClock()}
	e.svc = ledgerapp.NewService(ledgerapp.Deps{
		Tx:       tx,
		Accounts: e.store,
		Holds:    e.store,
		Journal:  e.store,
		Clock:    e.clock,
		Metrics:  ledgermetrics.NewBusiness(prometheus.NewRegistry()),
		NewID:    uuid.NewV7,
		Logger:   discard(),
	}, ledgerapp.Config{DefaultHoldTTL: 15 * time.Minute, MaxHoldTTL: 24 * time.Hour})
	return e
}

func (e *ledgerEnv) open(t *testing.T, name string, deposit int64) ledger.Account {
	t.Helper()
	res, err := e.svc.OpenAccount(context.Background(), ledgerapp.OpenAccountRequest{
		IdempotencyKey: "open:" + name, Name: name, Currency: "EUR", InitialDepositMinor: deposit,
	})
	if err != nil {
		t.Fatalf("OpenAccount(%s): %v", name, err)
	}
	return res.Account
}

// checkLedgerBooks verifies the invariants of the books with SQL, from the
// tables themselves rather than through the store.
func checkLedgerBooks(t *testing.T, wantCustomerTotal int64) {
	t.Helper()
	if n := count(t, ledgerDB, `SELECT count(*) FROM (
		SELECT transaction_id FROM entries GROUP BY transaction_id HAVING sum(amount_minor) <> 0) x`); n != 0 {
		t.Errorf("%d transactions do not balance", n)
	}
	if n := count(t, ledgerDB, `SELECT count(*) FROM transactions t
		WHERE (SELECT count(*) FROM entries e WHERE e.transaction_id = t.id) < 2`); n != 0 {
		t.Errorf("%d transactions have fewer than two entries", n)
	}
	if total := count(t, ledgerDB, `SELECT coalesce(sum(posted_minor), 0) FROM accounts`); total != 0 {
		t.Errorf("posted balances sum to %d, want 0: money was created or destroyed", total)
	}
	if got := count(t, ledgerDB, `SELECT coalesce(sum(posted_minor), 0) FROM accounts WHERE kind = 'customer'`); got != wantCustomerTotal {
		t.Errorf("customer money = %d, want %d", got, wantCustomerTotal)
	}
	if n := count(t, ledgerDB, `SELECT count(*) FROM accounts a
		WHERE a.posted_minor <> (SELECT coalesce(sum(amount_minor), 0) FROM entries e WHERE e.account_id = a.id)`); n != 0 {
		t.Errorf("%d accounts disagree with their entries", n)
	}
	if n := count(t, ledgerDB, `SELECT count(*) FROM accounts a
		WHERE a.held_minor <> (SELECT coalesce(sum(amount_minor), 0) FROM holds h WHERE h.account_id = a.id AND h.status = 'active')`); n != 0 {
		t.Errorf("%d accounts disagree with their active holds", n)
	}
	if n := count(t, ledgerDB, `SELECT count(*) FROM accounts WHERE kind = 'customer' AND posted_minor - held_minor < 0`); n != 0 {
		t.Errorf("%d customers have a negative available balance", n)
	}
}

// TestLedgerInvariantUnderConcurrentTransfers runs 100 transfers at once
// between ten accounts: each places a hold and then captures it, or
// releases it one time in five. Holds that would overdraw are refused.
// Whatever the interleaving, the books must balance.
func TestLedgerInvariantUnderConcurrentTransfers(t *testing.T) {
	e := newLedgerEnv(t)
	const accounts, deposit, transfers = 10, 10_000, 100
	var ids []uuid.UUID
	for i := range accounts {
		ids = append(ids, e.open(t, fmt.Sprint("pony-", i), deposit).ID)
	}

	var (
		wg                          sync.WaitGroup
		captured, released, refused atomic.Int64
		start                       = make(chan struct{})
		errs                        = make(chan error, transfers)
	)
	for i := range transfers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 7))
			src := rng.IntN(accounts)
			dst := (src + 1 + rng.IntN(accounts-1)) % accounts
			ctx := context.Background()
			<-start
			hold, err := e.svc.PlaceHold(ctx, ledgerapp.PlaceHoldRequest{
				IdempotencyKey: fmt.Sprint("transfer:", i, ":hold"), AccountID: ids[src], DestinationID: ids[dst],
				AmountMinor: 1 + rng.Int64N(4_000), Currency: "EUR",
			})
			if errors.Is(err, ledger.ErrInsufficientFunds) {
				refused.Add(1)
				return
			}
			if err != nil {
				errs <- fmt.Errorf("transfer %d: place: %w", i, err)
				return
			}
			if rng.IntN(5) == 0 {
				_, err = e.svc.ReleaseHold(ctx, ledgerapp.ReleaseHoldRequest{
					IdempotencyKey: fmt.Sprint("transfer:", i, ":release"), HoldID: hold.Hold.ID})
				released.Add(1)
			} else {
				_, err = e.svc.CaptureHold(ctx, ledgerapp.CaptureHoldRequest{
					IdempotencyKey: fmt.Sprint("transfer:", i, ":capture"), HoldID: hold.Hold.ID})
				captured.Add(1)
			}
			if err != nil {
				errs <- fmt.Errorf("transfer %d: settle: %w", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if got := captured.Load() + released.Load() + refused.Load(); got != transfers {
		t.Fatalf("outcomes %d, want %d", got, transfers)
	}
	checkLedgerBooks(t, accounts*deposit)
	if n := count(t, ledgerDB, `SELECT count(*) FROM transactions WHERE kind = 'transfer'`); n != captured.Load() {
		t.Errorf("transfer transactions = %d, captures = %d", n, captured.Load())
	}
	if n := count(t, ledgerDB, `SELECT count(*) FROM holds WHERE status = 'active'`); n != 0 {
		t.Errorf("%d holds left active", n)
	}
}

// Fifty holds of 100 race for an available balance of 1000: exactly ten are
// placed, and available ends at zero, never below.
func TestLedgerHoldsNeverOverdrawUnderConcurrency(t *testing.T) {
	e := newLedgerEnv(t)
	src, dst := e.open(t, "src", 1_000), e.open(t, "dst", 0)

	var (
		wg              sync.WaitGroup
		placed, refused atomic.Int64
		start           = make(chan struct{})
	)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := e.svc.PlaceHold(context.Background(), ledgerapp.PlaceHoldRequest{
				IdempotencyKey: fmt.Sprint("h", i), AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 100, Currency: "EUR",
			})
			switch {
			case err == nil:
				placed.Add(1)
			case errors.Is(err, ledger.ErrInsufficientFunds):
				refused.Add(1)
			default:
				t.Errorf("PlaceHold: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if placed.Load() != 10 || refused.Load() != 40 {
		t.Fatalf("placed %d, refused %d; want 10 and 40", placed.Load(), refused.Load())
	}
	a, err := e.svc.Account(context.Background(), src.ID)
	if err != nil {
		t.Fatal(err)
	}
	if a.AvailableMinor() != 0 || a.HeldMinor != 1_000 {
		t.Fatalf("source: available %d, held %d", a.AvailableMinor(), a.HeldMinor)
	}
	checkLedgerBooks(t, 1_000)
}

// Concurrent replays of the same request: one call does the work, every
// other one gets the same result back as a replay.
func TestLedgerConcurrentReplays(t *testing.T) {
	e := newLedgerEnv(t)
	src, dst := e.open(t, "src", 1_000), e.open(t, "dst", 0)
	const callers = 16

	run := func(call func() (replayed bool, id uuid.UUID, err error)) (firsts int, ids map[uuid.UUID]bool) {
		var (
			mu    sync.Mutex
			wg    sync.WaitGroup
			start = make(chan struct{})
		)
		ids = map[uuid.UUID]bool{}
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				replayed, id, err := call()
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					t.Errorf("call: %v", err)
					return
				}
				if !replayed {
					firsts++
				}
				ids[id] = true
			}()
		}
		close(start)
		wg.Wait()
		return firsts, ids
	}

	firsts, ids := run(func() (bool, uuid.UUID, error) {
		res, err := e.svc.PlaceHold(context.Background(), ledgerapp.PlaceHoldRequest{
			IdempotencyKey: "transfer:1:hold", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 300, Currency: "EUR"})
		return res.Replayed, res.Hold.ID, err
	})
	if firsts != 1 || len(ids) != 1 {
		t.Fatalf("PlaceHold: %d first calls, %d distinct holds; want 1 and 1", firsts, len(ids))
	}
	var holdID uuid.UUID
	for id := range ids {
		holdID = id
	}

	firsts, ids = run(func() (bool, uuid.UUID, error) {
		res, err := e.svc.CaptureHold(context.Background(), ledgerapp.CaptureHoldRequest{
			IdempotencyKey: "transfer:1:capture", HoldID: holdID})
		return res.Replayed, res.Transaction.ID, err
	})
	if firsts != 1 || len(ids) != 1 {
		t.Fatalf("CaptureHold: %d first calls, %d distinct transactions; want 1 and 1", firsts, len(ids))
	}
	if a, _ := e.svc.Account(context.Background(), src.ID); a.PostedMinor != 700 {
		t.Fatalf("source posted %d, want 700: debited exactly once", a.PostedMinor)
	}

	other, err := e.svc.PlaceHold(context.Background(), ledgerapp.PlaceHoldRequest{
		IdempotencyKey: "transfer:2:hold", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 200, Currency: "EUR"})
	if err != nil {
		t.Fatal(err)
	}
	firsts, _ = run(func() (bool, uuid.UUID, error) {
		res, err := e.svc.ReleaseHold(context.Background(), ledgerapp.ReleaseHoldRequest{
			IdempotencyKey: "transfer:2:release", HoldID: other.Hold.ID, Reason: "declined"})
		return res.Replayed, res.Hold.ID, err
	})
	if firsts != 1 {
		t.Fatalf("ReleaseHold: %d first calls, want 1", firsts)
	}
	checkLedgerBooks(t, 1_000)
}

// The expiry sweep releases holds the saga forgot, and a capture of an
// expired hold is refused even before the sweep runs.
func TestLedgerExpirySweep(t *testing.T) {
	e := newLedgerEnv(t)
	src, dst := e.open(t, "src", 1_000), e.open(t, "dst", 0)
	expiring, err := e.svc.PlaceHold(context.Background(), ledgerapp.PlaceHoldRequest{
		IdempotencyKey: "short", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 400, Currency: "EUR", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.PlaceHold(context.Background(), ledgerapp.PlaceHoldRequest{
		IdempotencyKey: "long", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 100, Currency: "EUR", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}

	e.clock.Advance(time.Minute)
	if _, err := e.svc.CaptureHold(context.Background(), ledgerapp.CaptureHoldRequest{
		IdempotencyKey: "cap", HoldID: expiring.Hold.ID}); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Fatalf("capture after expiry: %v, want ErrHoldExpired", err)
	}
	n, err := e.svc.ReleaseExpiredHolds(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("sweep released %d, %v; want 1", n, err)
	}
	a, _ := e.svc.Account(context.Background(), src.ID)
	if a.HeldMinor != 100 || a.AvailableMinor() != 900 {
		t.Fatalf("after the sweep: held %d, available %d", a.HeldMinor, a.AvailableMinor())
	}
	checkLedgerBooks(t, 1_000)
}

// The database refuses what the code must never do: an unbalanced posting,
// a change to the journal, and a customer overdraft.
func TestLedgerDatabaseGuards(t *testing.T) {
	e := newLedgerEnv(t)
	src := e.open(t, "src", 1_000)
	ctx := context.Background()

	tx, err := ledgerDB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO transactions (id, kind, idempotency_key, currency, created_at)
		VALUES ($1, 'deposit', 'unbalanced', 'EUR', now())`, txID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO entries (transaction_id, account_id, amount_minor, balance_after_minor, created_at)
		VALUES ($1, $2, 5, 1005, now())`, txID, src.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("commit of an unbalanced transaction must fail")
	}

	if _, err := ledgerDB.Exec(ctx, `UPDATE entries SET amount_minor = amount_minor + 1`); err == nil {
		t.Fatal("entries must be append-only")
	}
	if _, err := ledgerDB.Exec(ctx, `UPDATE accounts SET held_minor = posted_minor + 1 WHERE id = $1`, src.ID); err == nil {
		t.Fatal("a customer's available balance must not go below zero")
	}
	if _, err := e.store.LockAccounts(ctx, src.ID); err == nil {
		t.Fatal("LockAccounts outside a transaction must fail")
	}
	checkLedgerBooks(t, 1_000)
}
