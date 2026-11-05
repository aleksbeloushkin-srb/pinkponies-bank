package transfers

import (
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
)

// MaxAmountMinor caps one transfer, as the ledger does.
const MaxAmountMinor int64 = 1_000_000_000

// currencies are the currencies the ledger supports. A transfer in any other
// currency is refused at the API instead of failing in the saga.
var currencies = []string{"EUR", "USD", "GBP", "CHF", "JPY"}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// Transfer moves an amount from one customer account to another through
// the saga hold, screen, capture (or release).
//
// Version grows by one with every committed change of the transfer: its
// status and its saga log. Long polls wait for it to move; events carry it
// so consumers can order and discard stale ones.
type Transfer struct {
	ID                   uuid.UUID
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             string
	Status               Status
	Reason               Reason
	Version              int64
	// HoldID is the ledger hold, once placed.
	HoldID uuid.UUID
	// LedgerTransactionID is the ledger transaction of the capture.
	LedgerTransactionID uuid.UUID
	// ScreeningID is the screening provider's reference.
	ScreeningID    string
	IdempotencyKey string
	RequestHash    string
	// Steps is the saga log, in the order the steps first started.
	Steps     []StepRecord
	CreatedAt time.Time
	UpdatedAt time.Time
}

// New builds a pending transfer with version 1.
func New(id, source, destination uuid.UUID, amountMinor int64, currency, idempotencyKey, requestHash string, now time.Time) (Transfer, error) {
	switch {
	case source == uuid.Nil:
		return Transfer{}, &ValidationError{Field: "source_account_id", Reason: "is required"}
	case destination == uuid.Nil:
		return Transfer{}, &ValidationError{Field: "destination_account_id", Reason: "is required"}
	case source == destination:
		return Transfer{}, &ValidationError{Field: "destination_account_id", Reason: "must differ from the source account"}
	case amountMinor <= 0 || amountMinor > MaxAmountMinor:
		return Transfer{}, &ValidationError{Field: "amount_minor", Reason: fmt.Sprintf("must be between 1 and %d", MaxAmountMinor)}
	case !currencyPattern.MatchString(currency) || !slices.Contains(currencies, currency):
		return Transfer{}, &ValidationError{Field: "currency", Reason: fmt.Sprintf("must be one of %v", currencies)}
	}
	return Transfer{
		ID:                   id,
		SourceAccountID:      source,
		DestinationAccountID: destination,
		AmountMinor:          amountMinor,
		Currency:             currency,
		Status:               StatusPending,
		Version:              1,
		IdempotencyKey:       idempotencyKey,
		RequestHash:          requestHash,
		CreatedAt:            now,
		UpdatedAt:            now,
	}, nil
}

// TransitionTo moves the transfer to next with a reason. Leaving a final
// status, or any move the state machine does not allow, is
// ErrInvalidTransition and leaves the transfer untouched. The caller bumps
// the version once per committed change (see Touch).
func (t *Transfer) TransitionTo(next Status, reason Reason) error {
	if !t.Status.CanTransitionTo(next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, next)
	}
	t.Status, t.Reason = next, reason
	return nil
}

// Step returns the saga log entry of s.
func (t Transfer) Step(s Step) (StepRecord, bool) {
	for _, r := range t.Steps {
		if r.Step == s {
			return r, true
		}
	}
	return StepRecord{}, false
}

// StartStep records an attempt of s. The first attempt adds the step to the
// log as running and is a visible change (it returns true); later attempts
// only count, so a retry or a poll does not wake every long poll.
func (t *Transfer) StartStep(s Step, now time.Time) (visible bool) {
	for i := range t.Steps {
		if t.Steps[i].Step == s {
			t.Steps[i].Attempts++
			t.Steps[i].UpdatedAt = now
			return false
		}
	}
	t.Steps = append(t.Steps, StepRecord{Step: s, State: StepRunning, Attempts: 1, StartedAt: now, UpdatedAt: now})
	return true
}

// FinishStep records the state and outcome of a started step. It reports
// whether anything changed: a second review answer is not a change.
func (t *Transfer) FinishStep(s Step, state StepState, outcome string, now time.Time) (changed bool, err error) {
	for i := range t.Steps {
		r := &t.Steps[i]
		if r.Step != s {
			continue
		}
		if r.Done() {
			return false, fmt.Errorf("transfers: step %s already finished as %s", s, r.State)
		}
		if r.State == state && r.Outcome == outcome {
			return false, nil
		}
		r.State, r.Outcome, r.UpdatedAt = state, outcome, now
		return true, nil
	}
	return false, fmt.Errorf("transfers: step %s was never started", s)
}

// Touch records one committed change: the version grows by one and the
// update time moves.
func (t *Transfer) Touch(now time.Time) {
	t.Version++
	t.UpdatedAt = now
}

// LogValue limits logs to an allowlist of fields; the idempotency key and
// the request hash are client data.
func (t Transfer) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", t.ID.String()),
		slog.String("status", string(t.Status)),
		slog.Int64("version", t.Version),
		slog.Int64("amount_minor", t.AmountMinor),
		slog.String("currency", t.Currency),
	)
}
