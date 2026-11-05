package transfers

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var allStatuses = []Status{StatusPending, StatusHeld, StatusCompleted, StatusRejected, StatusCanceled}

func TestStatusTransitions(t *testing.T) {
	want := map[[2]Status]bool{
		{StatusPending, StatusHeld}:     true,
		{StatusPending, StatusRejected}: true,
		{StatusHeld, StatusCompleted}:   true,
		{StatusHeld, StatusRejected}:    true,
		{StatusHeld, StatusCanceled}:    true,
	}
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			if got := from.CanTransitionTo(to); got != want[[2]Status{from, to}] {
				t.Errorf("%s -> %s = %v", from, to, got)
			}
		}
		if from.Final() != (len(transitions[from]) == 0) {
			t.Errorf("%s: Final() = %v but has %d exits", from, from.Final(), len(transitions[from]))
		}
	}
}

func TestEveryStatusHasAnEvent(t *testing.T) {
	for _, s := range allStatuses {
		if eventByStatus[s] == "" {
			t.Errorf("status %s has no event type", s)
		}
	}
}

func newTransfer(t *testing.T) Transfer {
	t.Helper()
	tr, err := New(uuid.New(), uuid.New(), uuid.New(), 1250, "EUR", "key", "hash", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestNewValidates(t *testing.T) {
	same := uuid.New()
	tests := []struct {
		name     string
		src, dst uuid.UUID
		amount   int64
		currency string
		field    string
	}{
		{"no source", uuid.Nil, uuid.New(), 1, "EUR", "source_account_id"},
		{"same account", same, same, 1, "EUR", "destination_account_id"},
		{"zero amount", uuid.New(), uuid.New(), 0, "EUR", "amount_minor"},
		{"too much", uuid.New(), uuid.New(), MaxAmountMinor + 1, "EUR", "amount_minor"},
		{"bad currency", uuid.New(), uuid.New(), 1, "PONY", "currency"},
		{"unsupported currency", uuid.New(), uuid.New(), 1, "XXX", "currency"},
	}
	for _, tt := range tests {
		_, err := New(uuid.New(), tt.src, tt.dst, tt.amount, tt.currency, "k", "h", time.Now())
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != tt.field {
			t.Errorf("%s: err = %v, want field %s", tt.name, err, tt.field)
		}
	}
}

func TestStepLog(t *testing.T) {
	tr := newTransfer(t)
	now := time.Unix(10, 0)
	if !tr.StartStep(StepScreen, now) {
		t.Fatal("the first start of a step is a visible change")
	}
	if tr.StartStep(StepScreen, now.Add(time.Second)) {
		t.Fatal("a retry only counts")
	}
	if changed, err := tr.FinishStep(StepScreen, StepWaiting, OutcomeReview, now); err != nil || !changed {
		t.Fatalf("first review: %v %v", changed, err)
	}
	if changed, err := tr.FinishStep(StepScreen, StepWaiting, OutcomeReview, now); err != nil || changed {
		t.Fatalf("a repeated review is not a change: %v %v", changed, err)
	}
	if changed, err := tr.FinishStep(StepScreen, StepSucceeded, OutcomeApprove, now); err != nil || !changed {
		t.Fatalf("approve: %v %v", changed, err)
	}
	if _, err := tr.FinishStep(StepScreen, StepFailed, OutcomeTimeout, now); err == nil {
		t.Fatal("a finished step must not change again")
	}
	if _, err := tr.FinishStep(StepCapture, StepSucceeded, OutcomeCaptured, now); err == nil {
		t.Fatal("a step that never started cannot finish")
	}
	rec, ok := tr.Step(StepScreen)
	if !ok || rec.Attempts != 2 || rec.Outcome != OutcomeApprove || !rec.Done() || !rec.StartedAt.Equal(now) {
		t.Fatalf("record = %+v", rec)
	}
}

func TestTransitionToRefusesLeavingFinal(t *testing.T) {
	tr := newTransfer(t)
	if err := tr.TransitionTo(StatusCompleted, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("pending -> completed: %v", err)
	}
	if err := tr.TransitionTo(StatusHeld, ""); err != nil {
		t.Fatal(err)
	}
	if err := tr.TransitionTo(StatusRejected, ReasonScreeningDeclined); err != nil {
		t.Fatal(err)
	}
	if err := tr.TransitionTo(StatusHeld, ""); !errors.Is(err, ErrInvalidTransition) || tr.Status != StatusRejected {
		t.Fatalf("leaving a final status: %v, status %s", err, tr.Status)
	}
	tr.Touch(time.Unix(5, 0))
	if tr.Version != 2 {
		t.Fatalf("version = %d", tr.Version)
	}
}

func TestStepKeyIsStable(t *testing.T) {
	id := uuid.MustParse("0192f0a0-0000-7000-8000-000000000002")
	if got := StepKey(id, StepCapture); got != "transfer:0192f0a0-0000-7000-8000-000000000002:capture" {
		t.Fatalf("StepKey = %q", got)
	}
}
