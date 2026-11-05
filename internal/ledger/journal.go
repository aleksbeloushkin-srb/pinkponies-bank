package ledger

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TransactionKind says why money moved.
type TransactionKind string

// Transaction kinds.
const (
	// KindDeposit funds a new customer account from the treasury.
	KindDeposit TransactionKind = "deposit"
	// KindTransfer captures a hold: source to destination.
	KindTransfer TransactionKind = "transfer"
)

// Transaction is one balanced posting: its entries sum to zero.
type Transaction struct {
	ID             uuid.UUID
	Kind           TransactionKind
	IdempotencyKey string
	// HoldID is the captured hold of a transfer; uuid.Nil for a deposit.
	HoldID    uuid.UUID
	Currency  Currency
	Entries   []Entry
	CreatedAt time.Time
}

// Entry is one line of the journal: an amount added to (positive) or taken
// from (negative) one account, and the account's posted balance after it.
type Entry struct {
	ID                int64
	TransactionID     uuid.UUID
	AccountID         uuid.UUID
	AmountMinor       int64
	BalanceAfterMinor int64
	CreatedAt         time.Time
}

// Leg is one account's side of a posting.
type Leg struct {
	Account     *Account
	AmountMinor int64
}

// Post builds a balanced transaction from legs and applies each leg to its
// account's posted balance. It refuses fewer than two legs, a zero amount,
// a currency other than the transaction's, legs that do not sum to zero and
// a leg that would take a customer's available balance below zero; on error
// no account is changed.
func Post(kind TransactionKind, id uuid.UUID, idempotencyKey string, holdID uuid.UUID, currency Currency,
	now time.Time, legs ...Leg) (Transaction, error) {
	if len(legs) < 2 {
		return Transaction{}, fmt.Errorf("%w: %d legs", ErrUnbalanced, len(legs))
	}
	var sum int64
	for _, l := range legs {
		if l.AmountMinor == 0 {
			return Transaction{}, fmt.Errorf("%w: zero amount for account %s", ErrUnbalanced, l.Account.ID)
		}
		if l.Account.Currency != currency {
			return Transaction{}, fmt.Errorf("%w: account %s is in %s, the transaction in %s",
				ErrCurrencyMismatch, l.Account.ID, l.Account.Currency, currency)
		}
		if l.Account.Kind == KindCustomer && l.Account.AvailableMinor()+l.AmountMinor < 0 {
			return Transaction{}, fmt.Errorf("%w: account %s", ErrInsufficientFunds, l.Account.ID)
		}
		sum += l.AmountMinor
	}
	if sum != 0 {
		return Transaction{}, fmt.Errorf("%w: sum is %d", ErrUnbalanced, sum)
	}

	tx := Transaction{
		ID:             id,
		Kind:           kind,
		IdempotencyKey: idempotencyKey,
		HoldID:         holdID,
		Currency:       currency,
		CreatedAt:      now,
		Entries:        make([]Entry, 0, len(legs)),
	}
	for _, l := range legs {
		l.Account.PostedMinor += l.AmountMinor
		l.Account.UpdatedAt = now
		tx.Entries = append(tx.Entries, Entry{
			TransactionID:     id,
			AccountID:         l.Account.ID,
			AmountMinor:       l.AmountMinor,
			BalanceAfterMinor: l.Account.PostedMinor,
			CreatedAt:         now,
		})
	}
	return tx, nil
}
