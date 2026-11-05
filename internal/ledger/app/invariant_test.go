package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// TestRandomOperationsKeepTheBooksBalanced runs a seeded random mix of holds,
// captures, releases, expiries and replays, and checks the ledger's
// invariants after every step:
//
//   - the posted balances of all accounts, treasury included, sum to zero;
//   - every account's posted balance equals the sum of its entries;
//   - every account's held balance equals the sum of its active holds;
//   - no customer's available balance is negative;
//   - every transaction's entries sum to zero.
//
// Concurrency is covered by the PostgreSQL tests in test/integration; this
// one covers the arithmetic of every path, including refusals that must
// leave nothing behind.
func TestRandomOperationsKeepTheBooksBalanced(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			f := newFixture()
			rng := rand.New(rand.NewPCG(seed, 42))
			var accounts []ledger.Account
			for i := range 4 {
				accounts = append(accounts, f.open(t, fmt.Sprint("acct-", i), rng.Int64N(5_000)))
			}
			var holds []uuid.UUID
			ctx := context.Background()
			for step := range 300 {
				key := fmt.Sprint("op-", step)
				switch op := rng.IntN(6); {
				case op <= 1:
					src, dst := rng.IntN(4), rng.IntN(4)
					if src == dst {
						continue
					}
					res, err := f.svc.PlaceHold(ctx, PlaceHoldRequest{
						IdempotencyKey: key, AccountID: accounts[src].ID, DestinationID: accounts[dst].ID,
						AmountMinor: 1 + rng.Int64N(2_000), Currency: "EUR", TTL: time.Duration(1+rng.IntN(10)) * time.Minute,
					})
					if err == nil {
						holds = append(holds, res.Hold.ID)
					} else if !errors.Is(err, ledger.ErrInsufficientFunds) {
						t.Fatalf("step %d: PlaceHold: %v", step, err)
					}
				case op == 2 && len(holds) > 0:
					h := holds[rng.IntN(len(holds))]
					_, err := f.svc.CaptureHold(ctx, CaptureHoldRequest{IdempotencyKey: "capture:" + h.String(), HoldID: h})
					if err != nil && !errors.Is(err, ledger.ErrHoldReleased) && !errors.Is(err, ledger.ErrHoldExpired) {
						t.Fatalf("step %d: CaptureHold: %v", step, err)
					}
				case op == 3 && len(holds) > 0:
					h := holds[rng.IntN(len(holds))]
					_, err := f.svc.ReleaseHold(ctx, ReleaseHoldRequest{IdempotencyKey: "release:" + h.String(), HoldID: h})
					if err != nil && !errors.Is(err, ledger.ErrHoldCaptured) {
						t.Fatalf("step %d: ReleaseHold: %v", step, err)
					}
				case op == 4:
					f.clock.Advance(time.Duration(rng.IntN(90)) * time.Second)
				case op == 5:
					if _, err := f.svc.ReleaseExpiredHolds(ctx, 3); err != nil {
						t.Fatalf("step %d: sweep: %v", step, err)
					}
				}
				checkBooks(t, f.store, step)
			}
		})
	}
}

func checkBooks(t *testing.T, s *memStore, step int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	posted := map[uuid.UUID]int64{}
	held := map[uuid.UUID]int64{}
	for _, e := range s.entries {
		posted[e.AccountID] += e.AmountMinor
	}
	for _, h := range s.holds {
		if h.Status == ledger.HoldActive {
			held[h.AccountID] += h.AmountMinor
		}
	}
	for _, a := range s.accounts {
		total += a.PostedMinor
		if a.PostedMinor != posted[a.ID] {
			t.Fatalf("step %d: account %s posted %d, entries sum %d", step, a.ID, a.PostedMinor, posted[a.ID])
		}
		if a.HeldMinor != held[a.ID] {
			t.Fatalf("step %d: account %s held %d, active holds sum %d", step, a.ID, a.HeldMinor, held[a.ID])
		}
		if a.Kind == ledger.KindCustomer && a.AvailableMinor() < 0 {
			t.Fatalf("step %d: account %s available %d", step, a.ID, a.AvailableMinor())
		}
	}
	if total != 0 {
		t.Fatalf("step %d: balances sum to %d, want 0", step, total)
	}
	for _, txn := range s.txns {
		var sum int64
		for _, e := range txn.Entries {
			sum += e.AmountMinor
		}
		if sum != 0 {
			t.Fatalf("step %d: transaction %s sums to %d", step, txn.ID, sum)
		}
	}
}
