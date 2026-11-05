package transfers

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Step is one step of the transfer saga.
type Step string

// Saga steps. hold, screen and capture run forward; release compensates a
// hold when the transfer cannot complete.
const (
	StepHold    Step = "hold"
	StepScreen  Step = "screen"
	StepCapture Step = "capture"
	StepRelease Step = "release"
)

// Steps lists every step in saga order.
var Steps = []Step{StepHold, StepScreen, StepCapture, StepRelease}

// StepState is the state of one step.
type StepState string

// Step states. running means a remote call may be in flight; waiting means
// the remote side asked us to come back (screening under review);
// succeeded and failed are final for the step.
const (
	StepRunning   StepState = "running"
	StepWaiting   StepState = "waiting"
	StepSucceeded StepState = "succeeded"
	StepFailed    StepState = "failed"
)

// Step outcomes, recorded with the state.
const (
	OutcomePlaced      = "placed"
	OutcomeApprove     = "approve"
	OutcomeDecline     = "decline"
	OutcomeReview      = "review"
	OutcomeUnavailable = "unavailable"
	OutcomeTimeout     = "timeout"
	OutcomeCaptured    = "captured"
	OutcomeHoldExpired = "hold_expired"
	OutcomeReleased    = "released"
)

// StepOutcomes lists the outcomes each step can record, the outcome label
// of saga_steps_finished_total.
var StepOutcomes = map[Step][]string{
	StepHold:    {OutcomePlaced, string(ReasonInsufficientFunds), string(ReasonAccountNotFound), string(ReasonCurrencyMismatch)},
	StepScreen:  {OutcomeApprove, OutcomeDecline, OutcomeReview, OutcomeUnavailable, OutcomeTimeout},
	StepCapture: {OutcomeCaptured, OutcomeHoldExpired},
	StepRelease: {OutcomeReleased},
}

// StepRecord is the saga log entry of one step.
type StepRecord struct {
	Step    Step
	State   StepState
	Outcome string
	// Attempts counts the times the step was started, including retries
	// after an ambiguous answer and polls of a review.
	Attempts  int
	StartedAt time.Time
	UpdatedAt time.Time
}

// Done reports whether the step has finished.
func (r StepRecord) Done() bool {
	return r.State == StepSucceeded || r.State == StepFailed
}

// StepKey is the idempotency key of a step's remote call. It is the same on
// every attempt, which is what makes a retry after an ambiguous answer
// safe: the remote side executes the step once.
func StepKey(id uuid.UUID, s Step) string {
	return fmt.Sprintf("transfer:%s:%s", id, s)
}
