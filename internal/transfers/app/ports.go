package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// The interfaces below are what the use cases need from the outside world.
// They are declared here, next to their only consumer, and each adapter
// implements one of them.

// TxRunner runs fn inside one database transaction carried in ctx.
type TxRunner interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock returns the current time. Use cases never call time.Now.
type Clock interface {
	Now() time.Time
}

// TransferStore stores transfers with their saga log.
type TransferStore interface {
	// Insert stores t unless a transfer with the same idempotency key
	// exists, and returns the stored one either way.
	Insert(ctx context.Context, t transfers.Transfer) (stored transfers.Transfer, inserted bool, err error)
	// Transfer returns transfers.ErrNotFound for an unknown id.
	Transfer(ctx context.Context, id uuid.UUID) (transfers.Transfer, error)
	// Lock reads the transfer and locks its row until the transaction in ctx
	// ends; it fails without a transaction.
	Lock(ctx context.Context, id uuid.UUID) (transfers.Transfer, error)
	// Update persists t and its saga log if the stored version still equals
	// expectedVersion, and returns transfers.ErrVersionConflict otherwise.
	Update(ctx context.Context, t transfers.Transfer, expectedVersion int64) error
}

// Enqueuer schedules saga steps on the task queue.
type Enqueuer interface {
	Enqueue(ctx context.Context, s taskqueue.Spec) (enqueued bool, err error)
}

// EventRecorder writes events to the outbox in the caller's transaction.
type EventRecorder interface {
	Record(ctx context.Context, e transfers.Event) error
}

// Ledger is the ledger service. Every write carries an idempotency key, and
// the ledger executes a key once: a replay returns the original result with
// Replayed set.
type Ledger interface {
	PlaceHold(ctx context.Context, req HoldRequest) (Hold, error)
	CaptureHold(ctx context.Context, holdID uuid.UUID, key string) (Capture, error)
	ReleaseHold(ctx context.Context, holdID uuid.UUID, key, reason string) (Release, error)
	// Accounts lists the customer accounts.
	Accounts(ctx context.Context) ([]Account, error)
}

// HoldRequest asks the ledger to reserve money for a transfer.
type HoldRequest struct {
	Key           string
	SourceID      uuid.UUID
	DestinationID uuid.UUID
	AmountMinor   int64
	Currency      string
	TTL           time.Duration
}

// Hold is a placed hold.
type Hold struct {
	ID       uuid.UUID
	Replayed bool
}

// Capture is a captured hold: the ledger transaction that moved the money.
type Capture struct {
	TransactionID uuid.UUID
	Replayed      bool
}

// Release is a released hold.
type Release struct {
	Replayed bool
}

// Account is a customer account as the ledger reports it.
type Account struct {
	ID             uuid.UUID
	Name           string
	Currency       string
	PostedMinor    int64
	HeldMinor      int64
	AvailableMinor int64
}

// Errors of the Ledger port. Adapters wrap them, so callers match with
// errors.Is.
var (
	// The ledger refused the hold; the transfer is rejected.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrAccountNotFound   = errors.New("ledger: account not found")
	ErrCurrencyMismatch  = errors.New("ledger: currency mismatch")
	// The hold is gone: expired, or released by the expiry sweep.
	ErrHoldExpired = errors.New("ledger: hold expired or released")
	// The hold was captured; it cannot be released.
	ErrHoldCaptured = errors.New("ledger: hold already captured")
	// The ledger could not answer, or the answer was lost: the request may
	// or may not have been executed. Retry with the same key.
	ErrLedgerUnavailable = errors.New("ledger: unavailable")
	// The ledger rejected the request as malformed or contradictory: a bug
	// on one side. Retrying gives the same answer.
	ErrLedgerContract = errors.New("ledger: contract violation")
)

// Screener is the external screening provider. It deduplicates by key: a
// repeated request gets the same screening, whose decision may have moved
// from review to final.
type Screener interface {
	Screen(ctx context.Context, req ScreeningRequest) (Screening, error)
}

// ScreeningRequest asks whether a transfer may go ahead.
type ScreeningRequest struct {
	Key           string
	TransferID    uuid.UUID
	SourceID      uuid.UUID
	DestinationID uuid.UUID
	AmountMinor   int64
	Currency      string
}

// Decision is a screening decision.
type Decision string

// Decisions.
const (
	DecisionApprove Decision = "approve"
	DecisionDecline Decision = "decline"
	// DecisionReview means "not decided yet, ask again later".
	DecisionReview Decision = "review"
)

// Screening is the provider's answer.
type Screening struct {
	ID       string
	Decision Decision
	Reason   string
	// RetryAfter is when to ask again about a review.
	RetryAfter time.Duration
}

// ErrScreeningUnavailable means the provider gave no decision: it refused
// the call, timed out, failed, or the answer was lost. The call may have
// been executed; the saga retries it with the same key, a bounded number of
// times.
var ErrScreeningUnavailable = errors.New("screening: unavailable")

// RetryAfterError is a failure that says when to try again, for example a
// 429 with Retry-After. The task queue waits at least After before the next
// attempt.
type RetryAfterError struct {
	After time.Duration
	Err   error
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped error.
func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter returns the delay the downstream system asked for.
func (e *RetryAfterError) RetryAfter() time.Duration { return e.After }

// Watcher lets a long poll wait until a transfer's version moves past the
// one the client has seen.
type Watcher interface {
	// Watch subscribes to changes of id beyond seenVersion. The channel
	// receives the newer version once, or is closed without a value when
	// the watcher shuts down. cancel must always be called.
	Watch(id uuid.UUID, seenVersion int64) (changes <-chan int64, cancel func(), err error)
}

// Metrics receives business measurements, only after the transaction that
// made the change has committed.
type Metrics interface {
	TransferCreated()
	// StatusChanged reports a committed status change; age is the time
	// since the transfer was created.
	StatusChanged(from, to transfers.Status, age time.Duration)
	// StepFinished reports a committed step outcome.
	StepFinished(step transfers.Step, outcome string)
}
