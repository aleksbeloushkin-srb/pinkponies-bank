package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

func customer(t *testing.T, posted int64) *Account {
	t.Helper()
	a, err := NewCustomerAccount(uuid.New(), "Clover Hayfield", "EUR", "key", "hash", t0)
	if err != nil {
		t.Fatal(err)
	}
	a.PostedMinor = posted
	return &a
}

func TestParseCurrency(t *testing.T) {
	for _, ok := range []string{"EUR", "JPY"} {
		if _, err := ParseCurrency(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "eur", "EURO", "XXX"} {
		var verr *ValidationError
		if _, err := ParseCurrency(bad); !errors.As(err, &verr) || verr.Field != "currency" {
			t.Errorf("%q: err = %v, want a currency ValidationError", bad, err)
		}
	}
}

func TestNewCustomerAccountValidatesName(t *testing.T) {
	for _, name := range []string{"", "tab\tinside", string(make([]rune, MaxNameLen+1))} {
		if _, err := NewCustomerAccount(uuid.New(), name, "EUR", "k", "h", t0); err == nil {
			t.Errorf("name %q accepted", name)
		}
	}
	if _, err := NewCustomerAccount(uuid.New(), "Pip Sugarmane-Ünal", "EUR", "k", "h", t0); err != nil {
		t.Errorf("unicode name rejected: %v", err)
	}
}

func TestReserveNeverLetsAvailableGoNegative(t *testing.T) {
	a := customer(t, 1000)
	if err := a.Reserve(600, t0); err != nil {
		t.Fatal(err)
	}
	if err := a.Reserve(401, t0); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	if err := a.Reserve(400, t0); err != nil {
		t.Fatal(err)
	}
	if a.AvailableMinor() != 0 || a.HeldMinor != 1000 {
		t.Fatalf("available=%d held=%d", a.AvailableMinor(), a.HeldMinor)
	}
	if err := a.Unreserve(1001, t0); err == nil {
		t.Fatal("unreserving more than is held must fail")
	}
}

func TestSystemAccountMayGoNegative(t *testing.T) {
	treasury := NewTreasury(uuid.New(), "EUR", t0)
	cust := customer(t, 0)
	tx, err := Post(KindDeposit, uuid.New(), "open:1", uuid.Nil, "EUR", t0,
		Leg{&treasury, -500}, Leg{cust, 500})
	if err != nil {
		t.Fatal(err)
	}
	if treasury.PostedMinor != -500 || cust.PostedMinor != 500 {
		t.Fatalf("treasury=%d customer=%d", treasury.PostedMinor, cust.PostedMinor)
	}
	if tx.Entries[0].BalanceAfterMinor != -500 || tx.Entries[1].BalanceAfterMinor != 500 {
		t.Fatalf("entries = %+v", tx.Entries)
	}
}

func TestPostRejectsAndLeavesAccountsUntouched(t *testing.T) {
	tests := []struct {
		name string
		legs func(a, b *Account) []Leg
		want error
	}{
		{"unbalanced", func(a, b *Account) []Leg { return []Leg{{a, -100}, {b, 99}} }, ErrUnbalanced},
		{"one leg", func(a, _ *Account) []Leg { return []Leg{{a, -100}} }, ErrUnbalanced},
		{"zero amount", func(a, b *Account) []Leg { return []Leg{{a, 0}, {b, 0}} }, ErrUnbalanced},
		{"overdraft", func(a, b *Account) []Leg { return []Leg{{a, -1001}, {b, 1001}} }, ErrInsufficientFunds},
		{"other currency", func(a, b *Account) []Leg {
			b.Currency = "USD"
			return []Leg{{a, -1}, {b, 1}}
		}, ErrCurrencyMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := customer(t, 1000), customer(t, 0)
			_, err := Post(KindTransfer, uuid.New(), "k", uuid.New(), "EUR", t0, tt.legs(a, b)...)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if a.PostedMinor != 1000 || b.PostedMinor != 0 {
				t.Fatalf("accounts changed: a=%d b=%d", a.PostedMinor, b.PostedMinor)
			}
		})
	}
}

// A capture unreserves and posts: the source's available balance does not
// change, its posted balance drops, and the destination gains the amount.
func TestCaptureKeepsAvailableAndMovesPosted(t *testing.T) {
	src, dst := customer(t, 1000), customer(t, 50)
	if err := src.Reserve(300, t0); err != nil {
		t.Fatal(err)
	}
	if err := src.Unreserve(300, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := Post(KindTransfer, uuid.New(), "k", uuid.New(), "EUR", t0, Leg{src, -300}, Leg{dst, 300}); err != nil {
		t.Fatal(err)
	}
	if src.PostedMinor != 700 || src.AvailableMinor() != 700 || dst.PostedMinor != 350 {
		t.Fatalf("src posted=%d available=%d dst=%d", src.PostedMinor, src.AvailableMinor(), dst.PostedMinor)
	}
}

func TestHoldLifecycle(t *testing.T) {
	newHold := func() Hold {
		h, err := NewHold(uuid.New(), uuid.New(), uuid.New(), 100, "EUR", "k", "h", t0, t0.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		return h
	}

	h := newHold()
	if err := h.CheckCapturable(t0.Add(time.Minute)); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("at expiry: %v, want ErrHoldExpired", err)
	}
	if err := h.CheckCapturable(t0); err != nil {
		t.Fatal(err)
	}
	h.MarkCaptured(uuid.New(), t0)
	if err := h.CheckCapturable(t0); !errors.Is(err, ErrHoldCaptured) {
		t.Fatalf("captured: %v", err)
	}
	if _, err := h.Release("late", "r", t0); !errors.Is(err, ErrHoldCaptured) {
		t.Fatalf("release of a captured hold: %v", err)
	}

	h = newHold()
	changed, err := h.Release("declined", "r1", t0)
	if err != nil || !changed {
		t.Fatalf("first release: %v %v", changed, err)
	}
	changed, err = h.Release("again", "r2", t0)
	if err != nil || changed {
		t.Fatalf("second release must be a no-op: %v %v", changed, err)
	}
	if h.ReleaseReason != "declined" || h.ReleaseKey != "r1" {
		t.Fatalf("a replay must not overwrite the first release: %+v", h)
	}
	if err := h.CheckCapturable(t0); !errors.Is(err, ErrHoldReleased) {
		t.Fatalf("released: %v", err)
	}
}

func TestNewHoldValidates(t *testing.T) {
	same := uuid.New()
	tests := []struct {
		name      string
		src, dst  uuid.UUID
		amount    int64
		expiresAt time.Time
		field     string
	}{
		{"zero amount", uuid.New(), uuid.New(), 0, t0.Add(time.Minute), "amount_minor"},
		{"too large", uuid.New(), uuid.New(), MaxAmountMinor + 1, t0.Add(time.Minute), "amount_minor"},
		{"same account", same, same, 1, t0.Add(time.Minute), "destination_account_id"},
		{"no ttl", uuid.New(), uuid.New(), 1, t0, "ttl"},
	}
	for _, tt := range tests {
		_, err := NewHold(uuid.New(), tt.src, tt.dst, tt.amount, "EUR", "k", "h", t0, tt.expiresAt)
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != tt.field {
			t.Errorf("%s: err = %v, want field %s", tt.name, err, tt.field)
		}
	}
}
