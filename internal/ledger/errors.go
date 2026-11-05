package ledger

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers match them with errors.Is; the gRPC adapter maps
// them to status codes in one place.
var (
	// ErrAccountNotFound means a referenced account does not exist.
	ErrAccountNotFound = errors.New("account not found")
	// ErrHoldNotFound means the referenced hold does not exist.
	ErrHoldNotFound = errors.New("hold not found")
	// ErrInsufficientFunds means the available balance is below the amount.
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrCurrencyMismatch means the accounts or the request use different
	// currencies.
	ErrCurrencyMismatch = errors.New("currency mismatch")
	// ErrHoldCaptured means the hold was already captured; it can be neither
	// captured again under another key nor released.
	ErrHoldCaptured = errors.New("hold already captured")
	// ErrHoldReleased means the hold was released (on request or because it
	// expired) and cannot be captured.
	ErrHoldReleased = errors.New("hold already released")
	// ErrHoldExpired means the hold is past its expiry time and cannot be
	// captured; it is released by the expiry sweep or by the next release.
	ErrHoldExpired = errors.New("hold expired")
	// ErrIdempotencyConflict means an idempotency key was reused with a
	// different request.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrUnbalanced means a transaction's entries do not sum to zero. It is
	// always a bug in the caller.
	ErrUnbalanced = errors.New("transaction entries do not balance")
)

// ValidationError reports a bad input field. It becomes InvalidArgument.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}
