// Package taskqueue is a PostgreSQL task queue: durable background
// commands claimed with FOR UPDATE SKIP LOCKED and a lease, run by a Worker
// with retries, backoff with jitter, postponement and a dead state.
//
// It is shared by the services that need background work: transfers runs
// its saga steps on it, notifications its deliveries. Each service keeps
// the queue in its own database; the table is created by the service's
// migrations (see the tasks table in internal/transfers/migrations).
//
// The contract with a Handler is the error it returns:
//
//	nil                 the task is done
//	RetryLater(d, ...)  the run worked, run the same task again after d
//	                    without counting a failed attempt (polling)
//	an error with a     a failed attempt; retry after at least that long
//	RetryAfter() method (the downstream system asked for it)
//	Permanent(err)      retrying cannot help: the task goes dead at once
//	any other error     a failed attempt; retry after exponential backoff
//
// After MaxAttempts failed attempts a task goes dead too. An attempt is
// counted when the task is claimed, not when it is settled, so a handler
// that kills its process (out of memory, a fatal runtime error) reaches the
// dead state as well instead of looping forever. Dead tasks are the queue's
// dead-letter state: logged at error level, counted, exported as the
// tasks_dead gauge, and kept until an operator acts.
package taskqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Spec describes a task to schedule.
type Spec struct {
	Kind string
	// AggregateID is the entity the task works on, such as a transfer id.
	AggregateID uuid.UUID
	// DedupKey makes enqueueing idempotent: (Kind, AggregateID, DedupKey)
	// is unique and a second insert is ignored.
	DedupKey string
	Payload  json.RawMessage
	RunAfter time.Time
}

// Task is a claimed command handed to a Handler.
type Task struct {
	ID          int64
	Kind        string
	AggregateID uuid.UUID
	DedupKey    string
	Payload     json.RawMessage
	// Attempts counts the earlier runs that did not succeed: failed runs and
	// runs that never settled because their process died. Postponed runs do
	// not count.
	Attempts int
	RunAfter time.Time
	// LeaseUntil is the lease deadline set at claim time. The queue also
	// uses it as a fencing token: settling a task whose lease was taken over
	// by another worker fails with ErrLeaseLost.
	LeaseUntil time.Time
}

// ErrLeaseLost means the task was claimed again by another worker after
// our lease expired, so our result must not be recorded.
var ErrLeaseLost = errors.New("task lease lost")

// RetryLaterError asks the worker to run the same task again after a delay
// without counting a failed attempt. Polling uses it: "not final yet" is
// not a failure.
type RetryLaterError struct {
	After  time.Duration
	Reason string
}

func (e *RetryLaterError) Error() string {
	return fmt.Sprintf("retry in %s: %s", e.After, e.Reason)
}

// RetryLater returns a *RetryLaterError.
func RetryLater(after time.Duration, reason string) error {
	return &RetryLaterError{After: after, Reason: reason}
}

// retryAfterer is implemented by errors that carry a downstream system's
// "try again in" hint, such as a 429 with Retry-After. The worker counts a
// failed attempt and waits at least that long, or longer if its own backoff
// says so.
type retryAfterer interface {
	RetryAfter() time.Duration
}

// PermanentError marks a failure that retrying cannot fix. The worker moves
// the task to the dead state at once.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }

// Unwrap returns the wrapped error.
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err in a *PermanentError.
func Permanent(err error) error {
	return &PermanentError{Err: err}
}
