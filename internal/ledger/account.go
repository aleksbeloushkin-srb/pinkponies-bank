package ledger

import (
	"fmt"
	"log/slog"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// AccountKind separates customer accounts from the bank's own accounts.
type AccountKind string

// Account kinds.
const (
	// KindCustomer accounts belong to clients. Their available balance never
	// goes below zero.
	KindCustomer AccountKind = "customer"
	// KindSystem accounts belong to the bank, one treasury per currency. A
	// deposit moves money from the treasury to a customer, so the treasury's
	// balance is negative: it is the money the bank has issued.
	KindSystem AccountKind = "system"
)

// MaxNameLen bounds an account name, in characters.
const MaxNameLen = 64

// Account holds money in one currency.
//
// PostedMinor is the sum of the account's entries. HeldMinor is the sum of
// its active holds. The available balance, what the client can still spend,
// is the difference.
type Account struct {
	ID          uuid.UUID
	Name        string
	Currency    Currency
	Kind        AccountKind
	PostedMinor int64
	HeldMinor   int64
	// IdempotencyKey and RequestHash identify the OpenAccount request that
	// created a customer account; system accounts have neither.
	IdempotencyKey string
	RequestHash    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewCustomerAccount builds an empty customer account.
func NewCustomerAccount(id uuid.UUID, name string, currency Currency, idempotencyKey, requestHash string, now time.Time) (Account, error) {
	if err := validateName(name); err != nil {
		return Account{}, err
	}
	return Account{
		ID:             id,
		Name:           name,
		Currency:       currency,
		Kind:           KindCustomer,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// NewTreasury builds the system account that funds deposits in currency.
func NewTreasury(id uuid.UUID, currency Currency, now time.Time) Account {
	return Account{
		ID:        id,
		Name:      "Treasury " + string(currency),
		Currency:  currency,
		Kind:      KindSystem,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// TreasuryKey is the unique key of the treasury account of currency.
func TreasuryKey(currency Currency) string {
	return "treasury:" + string(currency)
}

func validateName(name string) error {
	n := utf8.RuneCountInString(name)
	if n == 0 || n > MaxNameLen || !utf8.ValidString(name) {
		return &ValidationError{Field: "name", Reason: fmt.Sprintf("must be 1 to %d characters", MaxNameLen)}
	}
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return &ValidationError{Field: "name", Reason: "must be printable"}
		}
	}
	return nil
}

// AvailableMinor is the posted balance minus active holds.
func (a Account) AvailableMinor() int64 {
	return a.PostedMinor - a.HeldMinor
}

// Reserve adds amountMinor to the held balance. A customer account cannot
// reserve more than it has available.
func (a *Account) Reserve(amountMinor int64, now time.Time) error {
	if a.Kind == KindCustomer && amountMinor > a.AvailableMinor() {
		return fmt.Errorf("%w: available %d, requested %d", ErrInsufficientFunds, a.AvailableMinor(), amountMinor)
	}
	a.HeldMinor += amountMinor
	a.UpdatedAt = now
	return nil
}

// Unreserve removes amountMinor from the held balance, when a hold is
// captured or released.
func (a *Account) Unreserve(amountMinor int64, now time.Time) error {
	if amountMinor > a.HeldMinor {
		return fmt.Errorf("ledger: unreserve %d from account %s holding %d", amountMinor, a.ID, a.HeldMinor)
	}
	a.HeldMinor -= amountMinor
	a.UpdatedAt = now
	return nil
}

// LogValue keeps logs to an allowlist of fields: the name is client data.
func (a Account) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", a.ID.String()),
		slog.String("kind", string(a.Kind)),
		slog.String("currency", string(a.Currency)),
	)
}
