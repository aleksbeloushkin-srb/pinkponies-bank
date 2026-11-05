package transfers

import (
	"errors"
	"fmt"
)

// Sentinel errors. The HTTP adapter maps them to status codes in one place.
var (
	// ErrNotFound means the transfer does not exist.
	ErrNotFound = errors.New("transfer not found")
	// ErrIdempotencyConflict means an Idempotency-Key was reused with a
	// different request body.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrInvalidTransition means the state machine rejected a status change.
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrVersionConflict means an optimistic version check failed.
	ErrVersionConflict = errors.New("version conflict")
)

// ValidationError reports a bad input field. It becomes HTTP 400.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}
