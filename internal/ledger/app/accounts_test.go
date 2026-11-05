package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

func (f *fixture) open(t *testing.T, key string, deposit int64) ledger.Account {
	t.Helper()
	res, err := f.svc.OpenAccount(context.Background(), OpenAccountRequest{
		IdempotencyKey: key, Name: "Account " + key, Currency: "EUR", InitialDepositMinor: deposit,
	})
	if err != nil {
		t.Fatalf("OpenAccount(%s): %v", key, err)
	}
	return res.Account
}

func TestOpenAccountFundsFromTheTreasury(t *testing.T) {
	f := newFixture()
	acct := f.open(t, "clover", 10_000)

	if acct.PostedMinor != 10_000 || acct.AvailableMinor() != 10_000 {
		t.Fatalf("account = %+v", acct)
	}
	page, err := f.svc.Accounts(context.Background(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	var treasury ledger.Account
	for _, a := range page.Items {
		total += a.PostedMinor
		if a.Kind == ledger.KindSystem {
			treasury = a
		}
	}
	if treasury.PostedMinor != -10_000 || total != 0 {
		t.Fatalf("treasury = %d, total = %d; money must be moved, not created", treasury.PostedMinor, total)
	}
	entries, err := f.svc.Entries(context.Background(), acct.ID, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries.Items) != 1 || entries.Items[0].AmountMinor != 10_000 || entries.Items[0].BalanceAfterMinor != 10_000 {
		t.Fatalf("entries = %+v", entries.Items)
	}
}

func TestOpenAccountIsIdempotent(t *testing.T) {
	f := newFixture()
	first := f.open(t, "clover", 500)

	res, err := f.svc.OpenAccount(context.Background(), OpenAccountRequest{
		IdempotencyKey: "clover", Name: "Account clover", Currency: "EUR", InitialDepositMinor: 500,
	})
	if err != nil || !res.Replayed || res.Account.ID != first.ID {
		t.Fatalf("replay = %+v, %v; want the first account", res, err)
	}
	if res.Account.PostedMinor != 500 {
		t.Fatal("a replay must not deposit again")
	}

	_, err = f.svc.OpenAccount(context.Background(), OpenAccountRequest{
		IdempotencyKey: "clover", Name: "Account clover", Currency: "EUR", InitialDepositMinor: 501,
	})
	if !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("same key, other deposit: err = %v", err)
	}
}

func TestOpenAccountValidates(t *testing.T) {
	f := newFixture()
	tests := []struct {
		name  string
		req   OpenAccountRequest
		field string
	}{
		{"no key", OpenAccountRequest{Name: "a", Currency: "EUR"}, "idempotency_key"},
		{"bad currency", OpenAccountRequest{IdempotencyKey: "k", Name: "a", Currency: "PONY"}, "currency"},
		{"negative deposit", OpenAccountRequest{IdempotencyKey: "k", Name: "a", Currency: "EUR", InitialDepositMinor: -1}, "initial_deposit_minor"},
		{"no name", OpenAccountRequest{IdempotencyKey: "k", Currency: "EUR"}, "name"},
	}
	for _, tt := range tests {
		_, err := f.svc.OpenAccount(context.Background(), tt.req)
		var verr *ledger.ValidationError
		if !errors.As(err, &verr) || verr.Field != tt.field {
			t.Errorf("%s: err = %v, want field %s", tt.name, err, tt.field)
		}
	}
}

func TestAccountsPagination(t *testing.T) {
	f := newFixture()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		f.open(t, k, 0)
	}
	var seen []uuid.UUID
	token := ""
	for range 10 {
		page, err := f.svc.Accounts(context.Background(), 2, token)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range page.Items {
			seen = append(seen, a.ID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	if len(seen) != 5 {
		t.Fatalf("listed %d accounts over all pages, want 5", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i].String() <= seen[i-1].String() {
			t.Fatal("accounts must come in id order without repeats")
		}
	}
}

func TestPageTokensAreCheckedPerList(t *testing.T) {
	f := newFixture()
	acct := f.open(t, "a", 100)
	if _, err := f.svc.Accounts(context.Background(), 0, "not base64!"); err == nil {
		t.Fatal("a malformed token must be rejected")
	}
	entriesToken := encodeToken(entriesTokenPrefix, "1")
	if _, err := f.svc.Accounts(context.Background(), 0, entriesToken); err == nil {
		t.Fatal("an entries token must not be accepted by the accounts list")
	}
	if _, err := f.svc.Entries(context.Background(), acct.ID, MaxPageSize+1, ""); err == nil {
		t.Fatal("an oversized page must be rejected")
	}
	if _, err := f.svc.Entries(context.Background(), uuid.New(), 0, ""); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("entries of an unknown account: %v", err)
	}
}
