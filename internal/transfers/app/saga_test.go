package app

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

func eventsOf(types ...transfers.EventType) []transfers.EventType { return types }

func TestHappyPath(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 1250)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusCompleted || tr.Reason != "" {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	for _, step := range []transfers.Step{transfers.StepHold, transfers.StepScreen, transfers.StepCapture} {
		if rec, ok := tr.Step(step); !ok || rec.State != transfers.StepSucceeded || rec.Attempts != 1 {
			t.Errorf("step %s = %+v", step, rec)
		}
	}
	if _, ok := tr.Step(transfers.StepRelease); ok {
		t.Error("a completed transfer has no release step")
	}
	if got := f.store.eventTypes(id); !slices.Equal(got, eventsOf(transfers.EventCreated, transfers.EventHeld, transfers.EventCompleted)) {
		t.Fatalf("events = %v", got)
	}
	if src, dst := f.ledger.account(f.src), f.ledger.account(f.dst); src.PostedMinor != 8_750 || src.HeldMinor != 0 || dst.PostedMinor != 1_250 {
		t.Fatalf("balances: src %+v dst %+v", src, dst)
	}
	// One version per committed change: created, hold started, held,
	// screen started, approved, capture started, completed.
	if tr.Version != 7 {
		t.Fatalf("version = %d, want 7", tr.Version)
	}
	if f.metrics.created != 1 || !slices.Equal(f.metrics.statuses, []string{"pending->held", "held->completed"}) {
		t.Fatalf("metrics = %+v", f.metrics)
	}
	for kind, state := range f.store.taskStates() {
		if state != "done" {
			t.Errorf("task %s is %s", kind, state)
		}
	}
}

func TestInsufficientFundsRejectsWithoutCompensation(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 10_001)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusRejected || tr.Reason != transfers.ReasonInsufficientFunds {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	if placed, captured, released := f.ledger.counts(); placed != 0 || captured != 0 || released != 0 {
		t.Fatalf("ledger calls executed: %d %d %d", placed, captured, released)
	}
	if n, _ := f.screener.stats(); n != 0 {
		t.Fatal("a refused hold must not be screened")
	}
	if got := f.store.eventTypes(id); !slices.Equal(got, eventsOf(transfers.EventCreated, transfers.EventRejected)) {
		t.Fatalf("events = %v", got)
	}
}

func TestUnknownAccountRejects(t *testing.T) {
	f := newFixture(t)
	f.dst = f.ledger.open("Ghost", 0)
	delete(f.ledger.accounts, f.dst)
	id := f.create(t, 100)
	f.drain(t)
	if tr := f.transfer(t, id); tr.Status != transfers.StatusRejected || tr.Reason != transfers.ReasonAccountNotFound {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
}

// A decline releases the hold: the money comes back and the transfer is
// rejected with a reason.
func TestDeclineCompensates(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 1266)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusRejected || tr.Reason != transfers.ReasonScreeningDeclined {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	if rec, _ := tr.Step(transfers.StepRelease); rec.State != transfers.StepSucceeded {
		t.Fatalf("release step = %+v", rec)
	}
	if _, captured, released := f.ledger.counts(); captured != 0 || released != 1 {
		t.Fatalf("captured %d, released %d", captured, released)
	}
	if src := f.ledger.account(f.src); src.PostedMinor != 10_000 || src.AvailableMinor != 10_000 {
		t.Fatalf("source after compensation: %+v", src)
	}
	if got := f.store.eventTypes(id); !slices.Equal(got, eventsOf(transfers.EventCreated, transfers.EventHeld, transfers.EventRejected)) {
		t.Fatalf("events = %v", got)
	}
}

// Ambiguous screening answers are retried with the same key; the provider
// screens the transfer once.
func TestScreeningAmbiguousRetriesWithTheSameKey(t *testing.T) {
	f := newFixture(t)
	failures := 0
	f.screener.fail = func() error {
		if failures < 3 {
			failures++
			return fmt.Errorf("%w: timeout after the request was written", ErrScreeningUnavailable)
		}
		return nil
	}
	id := f.create(t, 1250)
	f.drain(t)

	if tr := f.transfer(t, id); tr.Status != transfers.StatusCompleted {
		t.Fatalf("status = %s", tr.Status)
	}
	executions, keys := f.screener.stats()
	if executions != 1 || len(keys) != 4 {
		t.Fatalf("executions %d over %d calls, want 1 over 4", executions, len(keys))
	}
	for _, k := range keys {
		if k != transfers.StepKey(id, transfers.StepScreen) {
			t.Fatalf("call with key %q", k)
		}
	}
}

// Calls that keep failing are bounded: the saga gives up, releases the hold
// and cancels.
func TestScreeningFailuresAreBounded(t *testing.T) {
	f := newFixture(t)
	f.screener.fail = func() error { return ErrScreeningUnavailable }
	id := f.create(t, 1250)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusCanceled || tr.Reason != transfers.ReasonScreeningUnavailable {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	if _, keys := f.screener.stats(); len(keys) != testConfig().ScreenMaxFailures {
		t.Fatalf("screening calls = %d, want %d", len(keys), testConfig().ScreenMaxFailures)
	}
	if _, captured, released := f.ledger.counts(); captured != 0 || released != 1 {
		t.Fatalf("captured %d, released %d", captured, released)
	}
}

// A review is polled as a delayed retry of the same task, without counting
// failures, and the provider's answer then decides.
func TestScreeningReviewIsPolled(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 1277) // reviewed for 10 s, then approved
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusCompleted {
		t.Fatalf("status = %s", tr.Status)
	}
	rec, _ := tr.Step(transfers.StepScreen)
	executions, keys := f.screener.stats()
	if executions != 1 || len(keys) < 2 || rec.Attempts != len(keys) {
		t.Fatalf("screen step %+v after %d calls (%d executions)", rec, len(keys), executions)
	}
	if f.metrics.steps["screen:review"] != 1 {
		t.Fatalf("a review must be reported once, steps = %v", f.metrics.steps)
	}
}

func TestScreeningReviewTimesOut(t *testing.T) {
	f := newFixture(t)
	f.screener.reviewDelay = time.Hour
	id := f.create(t, 1277)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusCanceled || tr.Reason != transfers.ReasonScreeningTimeout {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	if src := f.ledger.account(f.src); src.AvailableMinor != 10_000 {
		t.Fatalf("hold not released: %+v", src)
	}
}

// A 429 says when to come back; the next attempt waits at least that long.
func TestRateLimitedScreeningHonoursRetryAfter(t *testing.T) {
	f := newFixture(t)
	var calls []time.Time
	f.screener.fail = func() error {
		calls = append(calls, f.clock.Now())
		if len(calls) == 1 {
			return &RetryAfterError{After: 20 * time.Second, Err: fmt.Errorf("%w: 429", ErrScreeningUnavailable)}
		}
		return nil
	}
	id := f.create(t, 1250)
	f.drain(t)
	if tr := f.transfer(t, id); tr.Status != transfers.StatusCompleted {
		t.Fatalf("status = %s", tr.Status)
	}
	if len(calls) != 2 || calls[1].Sub(calls[0]) < 20*time.Second {
		t.Fatalf("calls at %v, want the second at least 20 s after the first", calls)
	}
}

// The hold expired while the transfer waited: capture is refused, the hold
// is released (a replay if the ledger's sweep did it) and the transfer is
// canceled. No money moves.
func TestHoldExpiredAtCaptureCancels(t *testing.T) {
	f := newFixture(t)
	f.ledger.fail = func(op string) error {
		if op == "capture" {
			f.clock.Advance(time.Hour) // the hold expires before the capture
		}
		return nil
	}
	id := f.create(t, 1250)
	f.drain(t)

	tr := f.transfer(t, id)
	if tr.Status != transfers.StatusCanceled || tr.Reason != transfers.ReasonHoldExpired {
		t.Fatalf("transfer = %s %s", tr.Status, tr.Reason)
	}
	if _, captured, _ := f.ledger.counts(); captured != 0 {
		t.Fatal("an expired hold must not be captured")
	}
	if src := f.ledger.account(f.src); src.PostedMinor != 10_000 || src.HeldMinor != 0 {
		t.Fatalf("source = %+v", src)
	}
}

// A transient ledger failure is retried with the same key; a contract
// violation stops the step for an operator.
func TestLedgerFailures(t *testing.T) {
	t.Run("unavailable is retried", func(t *testing.T) {
		f := newFixture(t)
		n := 0
		f.ledger.fail = func(op string) error {
			if op == "capture" && n < 2 {
				n++
				return fmt.Errorf("%w: Unavailable", ErrLedgerUnavailable)
			}
			return nil
		}
		id := f.create(t, 1250)
		f.drain(t)
		if tr := f.transfer(t, id); tr.Status != transfers.StatusCompleted {
			t.Fatalf("status = %s", tr.Status)
		}
		if rec, _ := f.transfer(t, id).Step(transfers.StepCapture); rec.Attempts != 3 {
			t.Fatalf("capture attempts = %d, want 3", rec.Attempts)
		}
	})
	t.Run("contract violation is permanent", func(t *testing.T) {
		f := newFixture(t)
		f.ledger.fail = func(op string) error {
			if op == "place" {
				return fmt.Errorf("%w: InvalidArgument: bad ttl", ErrLedgerContract)
			}
			return nil
		}
		id := f.create(t, 1250)
		f.drain(t)
		if tr := f.transfer(t, id); tr.Status != transfers.StatusPending {
			t.Fatalf("status = %s, want pending until an operator acts", tr.Status)
		}
		if got := f.store.taskStates()[KindHold]; got != "dead" {
			t.Fatalf("hold task = %s, want dead", got)
		}
	})
}

// A worker whose lease expired may finish a step another worker finished
// already: the second result is ignored and nothing is applied twice.
func TestObsoleteTasksAreIgnored(t *testing.T) {
	f := newFixture(t)
	id := f.create(t, 1250)
	f.drain(t)
	before := f.transfer(t, id)

	for _, kind := range TaskKinds {
		task := taskqueue.Task{Kind: kind, AggregateID: id, Payload: []byte(`{"status":"rejected","reason":"screening_declined"}`)}
		if err := f.svc.HandleTask(t.Context(), task); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	after := f.transfer(t, id)
	if after.Version != before.Version || after.Status != transfers.StatusCompleted {
		t.Fatalf("an obsolete task changed the transfer: v%d -> v%d, %s", before.Version, after.Version, after.Status)
	}
	if _, captured, released := f.ledger.counts(); captured != 1 || released != 0 {
		t.Fatalf("captured %d, released %d", captured, released)
	}
}

func TestUnknownTaskKindIsPermanent(t *testing.T) {
	f := newFixture(t)
	err := f.svc.HandleTask(t.Context(), taskqueue.Task{Kind: "teleport", AggregateID: uuid.New()})
	var perm *taskqueue.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("err = %v, want permanent", err)
	}
}
