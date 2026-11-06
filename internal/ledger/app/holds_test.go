package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

func (f *fixture) place(t *testing.T, key string, src, dst ledger.Account, amount int64) HoldResult {
	t.Helper()
	res, err := f.svc.PlaceHold(context.Background(), PlaceHoldRequest{
		IdempotencyKey: key, AccountID: src.ID, DestinationID: dst.ID, AmountMinor: amount, Currency: "EUR",
	})
	if err != nil {
		t.Fatalf("PlaceHold(%s): %v", key, err)
	}
	return res
}

func (f *fixture) account(t *testing.T, id uuid.UUID) ledger.Account {
	t.Helper()
	a, err := f.svc.Account(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPlaceHoldReservesAndReplays(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)

	first := f.place(t, "transfer:1:hold", src, dst, 600)
	if first.Replayed || first.Hold.Status != ledger.HoldActive || !first.Hold.ExpiresAt.Equal(t0.Add(15*time.Minute)) {
		t.Fatalf("hold = %+v", first)
	}
	if a := f.account(t, src.ID); a.HeldMinor != 600 || a.AvailableMinor() != 400 || a.PostedMinor != 1000 {
		t.Fatalf("source after hold = %+v", a)
	}

	// A replay returns the same hold and reserves nothing more, even with
	// another TTL.
	res, err := f.svc.PlaceHold(context.Background(), PlaceHoldRequest{
		IdempotencyKey: "transfer:1:hold", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 600, Currency: "EUR", TTL: time.Hour,
	})
	if err != nil || !res.Replayed || res.Hold.ID != first.Hold.ID || !res.Hold.ExpiresAt.Equal(first.Hold.ExpiresAt) {
		t.Fatalf("replay = %+v, %v", res, err)
	}
	if a := f.account(t, src.ID); a.HeldMinor != 600 {
		t.Fatalf("held after replay = %d, want 600", a.HeldMinor)
	}
	if f.metrics.placed != 1 {
		t.Fatalf("holds placed counted = %d, want 1 (replays are not counted)", f.metrics.placed)
	}

	_, err = f.svc.PlaceHold(context.Background(), PlaceHoldRequest{
		IdempotencyKey: "transfer:1:hold", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 601, Currency: "EUR",
	})
	if !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("same key, other amount: %v", err)
	}
}

func TestPlaceHoldRefusals(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)
	usd, err := f.svc.OpenAccount(context.Background(), OpenAccountRequest{IdempotencyKey: "usd", Name: "Dollar", Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		req  PlaceHoldRequest
		want error
	}{
		{"insufficient funds", PlaceHoldRequest{AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 1001, Currency: "EUR"}, ledger.ErrInsufficientFunds},
		{"unknown source", PlaceHoldRequest{AccountID: uuid.New(), DestinationID: dst.ID, AmountMinor: 1, Currency: "EUR"}, ledger.ErrAccountNotFound},
		{"unknown destination", PlaceHoldRequest{AccountID: src.ID, DestinationID: uuid.New(), AmountMinor: 1, Currency: "EUR"}, ledger.ErrAccountNotFound},
		{"destination in another currency", PlaceHoldRequest{AccountID: src.ID, DestinationID: usd.Account.ID, AmountMinor: 1, Currency: "EUR"}, ledger.ErrCurrencyMismatch},
		{"request in another currency", PlaceHoldRequest{AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 1, Currency: "USD"}, ledger.ErrCurrencyMismatch},
	}
	for i, tt := range tests {
		tt.req.IdempotencyKey = "k" + string(rune('a'+i))
		if _, err := f.svc.PlaceHold(context.Background(), tt.req); !errors.Is(err, tt.want) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
		}
	}
	if a := f.account(t, src.ID); a.HeldMinor != 0 {
		t.Fatalf("a refused hold must reserve nothing, held = %d", a.HeldMinor)
	}

	for name, req := range map[string]PlaceHoldRequest{
		"same account": {IdempotencyKey: "x", AccountID: src.ID, DestinationID: src.ID, AmountMinor: 1, Currency: "EUR"},
		"ttl too long": {IdempotencyKey: "y", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 1, Currency: "EUR", TTL: 25 * time.Hour},
		"zero amount":  {IdempotencyKey: "z", AccountID: src.ID, DestinationID: dst.ID, Currency: "EUR"},
	} {
		var verr *ledger.ValidationError
		if _, err := f.svc.PlaceHold(context.Background(), req); !errors.As(err, &verr) {
			t.Errorf("%s: err = %v, want a ValidationError", name, err)
		}
	}
}

func TestCaptureMovesMoneyOnce(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 100)
	hold := f.place(t, "transfer:1:hold", src, dst, 300).Hold

	res, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "transfer:1:capture", HoldID: hold.ID})
	if err != nil || res.Replayed || res.Hold.Status != ledger.HoldCaptured {
		t.Fatalf("capture = %+v, %v", res, err)
	}
	if len(res.Transaction.Entries) != 2 || res.Transaction.Entries[0].AmountMinor+res.Transaction.Entries[1].AmountMinor != 0 {
		t.Fatalf("entries = %+v, want two that balance", res.Transaction.Entries)
	}
	a, b := f.account(t, src.ID), f.account(t, dst.ID)
	if a.PostedMinor != 700 || a.HeldMinor != 0 || b.PostedMinor != 400 {
		t.Fatalf("after capture: src %+v, dst %+v", a, b)
	}

	replay, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "transfer:1:capture", HoldID: hold.ID})
	if err != nil || !replay.Replayed || replay.Transaction.ID != res.Transaction.ID {
		t.Fatalf("replay = %+v, %v; want the original transaction", replay, err)
	}
	if a := f.account(t, src.ID); a.PostedMinor != 700 {
		t.Fatalf("a replay must not debit again: posted = %d", a.PostedMinor)
	}
	if f.metrics.captured != 1 {
		t.Fatalf("captures counted = %d, want 1", f.metrics.captured)
	}

	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "other-key", HoldID: hold.ID}); !errors.Is(err, ledger.ErrHoldCaptured) {
		t.Fatalf("capture under another key: %v", err)
	}
	if _, err := f.svc.ReleaseHold(context.Background(), ReleaseHoldRequest{IdempotencyKey: "r", HoldID: hold.ID}); !errors.Is(err, ledger.ErrHoldCaptured) {
		t.Fatalf("release of a captured hold: %v", err)
	}
}

func TestCaptureKeyReusedForAnotherHold(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)
	h1 := f.place(t, "h1", src, dst, 10).Hold
	h2 := f.place(t, "h2", src, dst, 10).Hold
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "cap", HoldID: h1.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "cap", HoldID: h2.ID}); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCaptureRefusesExpiredAndReleasedHolds(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)

	expired := f.place(t, "h1", src, dst, 100).Hold
	f.clock.Advance(15 * time.Minute)
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "c1", HoldID: expired.ID}); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Fatalf("expired: %v", err)
	}

	released := f.place(t, "h2", src, dst, 100).Hold
	if _, err := f.svc.ReleaseHold(context.Background(), ReleaseHoldRequest{IdempotencyKey: "r2", HoldID: released.ID, Reason: "declined"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "c2", HoldID: released.ID}); !errors.Is(err, ledger.ErrHoldReleased) {
		t.Fatalf("released: %v", err)
	}
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "c3", HoldID: uuid.New()}); !errors.Is(err, ledger.ErrHoldNotFound) {
		t.Fatalf("unknown hold: %v", err)
	}
	if a := f.account(t, src.ID); a.PostedMinor != 1000 {
		t.Fatalf("no capture may have posted: %d", a.PostedMinor)
	}
}

// A capture that waits for the hold's row lock past the expiry is refused:
// the expiry is checked against the time after the wait.
func TestCaptureChecksExpiryAfterTheLockWait(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)
	hold := f.place(t, "h1", src, dst, 100).Hold
	f.clock.Advance(15*time.Minute - time.Second)
	f.store.onLock = func() { f.clock.Advance(2 * time.Second) }
	if _, err := f.svc.CaptureHold(context.Background(), CaptureHoldRequest{IdempotencyKey: "c1", HoldID: hold.ID}); !errors.Is(err, ledger.ErrHoldExpired) {
		t.Fatalf("capture after a lock wait past the expiry = %v, want ErrHoldExpired", err)
	}
}

func TestReleaseIsIdempotentWhoeverReleasedFirst(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)
	hold := f.place(t, "h", src, dst, 400).Hold

	res, err := f.svc.ReleaseHold(context.Background(), ReleaseHoldRequest{IdempotencyKey: "transfer:1:release", HoldID: hold.ID, Reason: "screening declined"})
	if err != nil || res.Replayed || res.Hold.Status != ledger.HoldReleased {
		t.Fatalf("release = %+v, %v", res, err)
	}
	if a := f.account(t, src.ID); a.HeldMinor != 0 || a.AvailableMinor() != 1000 {
		t.Fatalf("after release: %+v", a)
	}
	again, err := f.svc.ReleaseHold(context.Background(), ReleaseHoldRequest{IdempotencyKey: "another-key", HoldID: hold.ID})
	if err != nil || !again.Replayed || again.Hold.ReleaseReason != "screening declined" {
		t.Fatalf("second release = %+v, %v; want a replay that keeps the first reason", again, err)
	}
	if f.metrics.releasedByCause[CauseRequested] != 1 {
		t.Fatalf("releases counted = %v", f.metrics.releasedByCause)
	}

	// A hold released by the expiry sweep is released for the saga too.
	swept := f.place(t, "h2", src, dst, 100).Hold
	f.clock.Advance(time.Hour)
	if n, err := f.svc.ReleaseExpiredHolds(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("sweep = %d, %v", n, err)
	}
	res, err = f.svc.ReleaseHold(context.Background(), ReleaseHoldRequest{IdempotencyKey: "transfer:2:release", HoldID: swept.ID})
	if err != nil || !res.Replayed || res.Hold.ReleaseReason != ledger.ReasonExpired {
		t.Fatalf("release after expiry = %+v, %v", res, err)
	}
}

func TestReleaseExpiredHolds(t *testing.T) {
	f := newFixture()
	src, dst := f.open(t, "src", 1000), f.open(t, "dst", 0)
	short, err := f.svc.PlaceHold(context.Background(), PlaceHoldRequest{
		IdempotencyKey: "short", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 100, Currency: "EUR", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	busy, err := f.svc.PlaceHold(context.Background(), PlaceHoldRequest{
		IdempotencyKey: "busy", AccountID: src.ID, DestinationID: dst.ID, AmountMinor: 100, Currency: "EUR", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	long := f.place(t, "long", src, dst, 100).Hold // default TTL, 15 minutes
	f.store.busy[busy.Hold.ID] = true

	f.clock.Advance(2 * time.Minute)
	n, err := f.svc.ReleaseExpiredHolds(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("released %d, %v; want only the expired hold that is not locked", n, err)
	}
	for id, want := range map[uuid.UUID]ledger.HoldStatus{short.Hold.ID: ledger.HoldReleased, busy.Hold.ID: ledger.HoldActive, long.ID: ledger.HoldActive} {
		if h, _ := f.store.LockHold(context.Background(), id); h.Status != want {
			t.Errorf("hold %s: %s, want %s", id, h.Status, want)
		}
	}
	if a := f.account(t, src.ID); a.HeldMinor != 200 {
		t.Fatalf("held = %d, want 200", a.HeldMinor)
	}

	delete(f.store.busy, busy.Hold.ID)
	if n, err := f.svc.ReleaseExpiredHolds(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("next sweep released %d, %v; want the formerly locked hold", n, err)
	}
	if f.metrics.releasedByCause[CauseExpired] != 2 {
		t.Fatalf("expired releases counted = %v", f.metrics.releasedByCause)
	}
}
