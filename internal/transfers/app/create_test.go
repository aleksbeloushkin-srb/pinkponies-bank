package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

func (f *fixture) request(key string, amount int64) CreateTransferRequest {
	return CreateTransferRequest{IdempotencyKey: key, SourceAccountID: f.src, DestinationAccountID: f.dst, AmountMinor: amount, Currency: "EUR"}
}

func TestCreateTransferIsIdempotent(t *testing.T) {
	f := newFixture(t)
	first, err := f.svc.CreateTransfer(context.Background(), f.request("k1", 500))
	if err != nil || !first.Created || first.Transfer.Status != transfers.StatusPending || first.Transfer.Version != 1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	replay, err := f.svc.CreateTransfer(context.Background(), f.request("k1", 500))
	if err != nil || replay.Created || replay.Transfer.ID != first.Transfer.ID {
		t.Fatalf("replay = %+v, %v; want the same transfer", replay, err)
	}
	if _, err := f.svc.CreateTransfer(context.Background(), f.request("k1", 501)); !errors.Is(err, transfers.ErrIdempotencyConflict) {
		t.Fatalf("same key, other amount: %v", err)
	}
	if f.metrics.created != 1 {
		t.Fatalf("created counted %d times, want 1", f.metrics.created)
	}
	if got := f.store.eventTypes(first.Transfer.ID); len(got) != 1 || got[0] != transfers.EventCreated {
		t.Fatalf("events = %v", got)
	}
	if f.store.taskStates()[KindHold] != "ready" {
		t.Fatal("the hold step must be scheduled with the transfer")
	}
}

// A failure inside the transaction leaves nothing behind: no transfer, no
// task, no event.
func TestCreateTransferIsAtomic(t *testing.T) {
	f := newFixture(t)
	f.store.crashBefore = 1
	func() {
		defer func() { _ = recover() }()
		_, _ = f.svc.CreateTransfer(context.Background(), f.request("k", 500))
	}()
	if len(f.store.transfers) != 0 || len(f.store.tasks) != 0 || len(f.store.events) != 0 {
		t.Fatalf("left behind: %d transfers, %d tasks, %d events", len(f.store.transfers), len(f.store.tasks), len(f.store.events))
	}
}

func TestCreateTransferValidates(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name  string
		req   CreateTransferRequest
		field string
	}{
		{"no key", f.request("", 1), "Idempotency-Key"},
		{"key not ASCII", f.request("ключ", 1), "Idempotency-Key"},
		{"zero amount", f.request("k", 0), "amount_minor"},
		{"same accounts", CreateTransferRequest{IdempotencyKey: "k", SourceAccountID: f.src, DestinationAccountID: f.src, AmountMinor: 1, Currency: "EUR"}, "destination_account_id"},
		{"no source", CreateTransferRequest{IdempotencyKey: "k", DestinationAccountID: uuid.New(), AmountMinor: 1, Currency: "EUR"}, "source_account_id"},
		{"currency", CreateTransferRequest{IdempotencyKey: "k", SourceAccountID: f.src, DestinationAccountID: f.dst, AmountMinor: 1, Currency: "eur"}, "currency"},
	}
	for _, tt := range tests {
		_, err := f.svc.CreateTransfer(context.Background(), tt.req)
		var verr *transfers.ValidationError
		if !errors.As(err, &verr) || verr.Field != tt.field {
			t.Errorf("%s: err = %v, want field %s", tt.name, err, tt.field)
		}
	}
}

func TestAccountsComeFromTheLedger(t *testing.T) {
	f := newFixture(t)
	accounts, err := f.svc.Accounts(context.Background())
	if err != nil || len(accounts) != 2 || accounts[0].Name != "Clover" {
		t.Fatalf("Accounts = %+v, %v", accounts, err)
	}
	f.ledger.fail = func(string) error { return ErrLedgerUnavailable }
	if _, err := f.svc.Accounts(context.Background()); !errors.Is(err, ErrLedgerUnavailable) {
		t.Fatalf("err = %v", err)
	}
}
