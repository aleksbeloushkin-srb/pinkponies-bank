package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// releasePayload is the payload of a release task: the status and reason
// the transfer ends with once its hold is released.
type releasePayload struct {
	Status transfers.Status `json:"status"`
	Reason transfers.Reason `json:"reason"`
}

// HandleTask runs one saga step; the queue worker calls it.
//
// Every step has the same shape, and the order is what makes it safe:
//
//  1. beginStep: a short transaction checks that the transfer is still at
//     this step and records an attempt;
//  2. the remote call, with no transaction open and the step's fixed
//     idempotency key, so a repeat after a crash or a lost answer is
//     executed once by the remote side;
//  3. finishStep: a second short transaction applies the answer, bumps the
//     version, records the event of a status change and enqueues the next
//     step, all at once.
//
// A crash anywhere leaves the task in the queue, and the next run repeats
// from step 1 with the same key. docs/SAGA.md has the table.
func (s *Service) HandleTask(ctx context.Context, task taskqueue.Task) error {
	// The transfer id is the request id of every log line and remote call of
	// the saga, so one search follows a transfer across services.
	ctx = platform.WithRequestID(ctx, task.AggregateID.String())
	switch task.Kind {
	case KindHold:
		return s.placeHold(ctx, task.AggregateID)
	case KindScreen:
		return s.screen(ctx, task.AggregateID, task.Attempts)
	case KindCapture:
		return s.capture(ctx, task.AggregateID)
	case KindRelease:
		return s.release(ctx, task.AggregateID, task.Payload)
	default:
		return taskqueue.Permanent(fmt.Errorf("unknown task kind %q", task.Kind))
	}
}

// placeHold reserves the amount on the source account. A refusal (no funds,
// unknown account, other currency) rejects the transfer; nothing needs
// compensating, because nothing was reserved.
func (s *Service) placeHold(ctx context.Context, id uuid.UUID) error {
	tr, ok, err := s.beginStep(ctx, id, transfers.StepHold, transfers.StatusPending)
	if err != nil || !ok {
		return err
	}
	hold, err := s.ledger.PlaceHold(ctx, HoldRequest{
		Key:           transfers.StepKey(id, transfers.StepHold),
		SourceID:      tr.SourceAccountID,
		DestinationID: tr.DestinationAccountID,
		AmountMinor:   tr.AmountMinor,
		Currency:      tr.Currency,
		TTL:           s.cfg.HoldTTL,
	})
	if reason, refused := holdRefusal(err); refused {
		return s.finishStep(ctx, id, transfers.StepHold, transfers.StatusPending,
			func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
				if _, err := tr.FinishStep(transfers.StepHold, transfers.StepFailed, string(reason), now); err != nil {
					return nil, err
				}
				return nil, tr.TransitionTo(transfers.StatusRejected, reason)
			})
	}
	if err != nil {
		return stepError(transfers.StepHold, err)
	}
	return s.finishStep(ctx, id, transfers.StepHold, transfers.StatusPending,
		func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
			tr.HoldID = hold.ID
			if _, err := tr.FinishStep(transfers.StepHold, transfers.StepSucceeded, transfers.OutcomePlaced, now); err != nil {
				return nil, err
			}
			if err := tr.TransitionTo(transfers.StatusHeld, ""); err != nil {
				return nil, err
			}
			next := s.step(KindScreen, tr.ID, nil)
			return &next, nil
		})
}

func holdRefusal(err error) (transfers.Reason, bool) {
	switch {
	case errors.Is(err, ErrInsufficientFunds):
		return transfers.ReasonInsufficientFunds, true
	case errors.Is(err, ErrAccountNotFound):
		return transfers.ReasonAccountNotFound, true
	case errors.Is(err, ErrCurrencyMismatch):
		return transfers.ReasonCurrencyMismatch, true
	}
	return "", false
}

// screen asks the provider about the transfer. Approve goes on to capture,
// decline compensates. An ambiguous answer (timeout, error, lost
// connection) is retried with the same key, so the provider screens the
// transfer once, up to ScreenMaxFailures failed calls; review is polled as
// a delayed retry of the same task. ScreenReviewTimeout, counted from the
// step's first run, bounds the whole step. Past either bound the hold is
// released and the transfer is canceled.
//
// failures is the number of failed runs of this task so far, counted by the
// queue; polls of a review are not failures.
func (s *Service) screen(ctx context.Context, id uuid.UUID, failures int) error {
	tr, ok, err := s.beginStep(ctx, id, transfers.StepScreen, transfers.StatusHeld)
	if err != nil || !ok {
		return err
	}
	if failures >= s.cfg.ScreenMaxFailures {
		return s.compensate(ctx, id, transfers.StepScreen, transfers.StepFailed, transfers.OutcomeUnavailable,
			transfers.StatusCanceled, transfers.ReasonScreeningUnavailable)
	}
	rec, _ := tr.Step(transfers.StepScreen)
	left := s.cfg.ScreenReviewTimeout - s.clock.Now().Sub(rec.StartedAt)
	if left <= 0 {
		// Out of time: a review that never ended, or failures that kept
		// asking the step to wait.
		reason := transfers.ReasonScreeningUnavailable
		if rec.Outcome == transfers.OutcomeReview {
			reason = transfers.ReasonScreeningTimeout
		}
		return s.compensate(ctx, id, transfers.StepScreen, transfers.StepFailed, transfers.OutcomeTimeout,
			transfers.StatusCanceled, reason)
	}

	res, err := s.screener.Screen(ctx, ScreeningRequest{
		Key:           transfers.StepKey(id, transfers.StepScreen),
		TransferID:    id,
		SourceID:      tr.SourceAccountID,
		DestinationID: tr.DestinationAccountID,
		AmountMinor:   tr.AmountMinor,
		Currency:      tr.Currency,
	})
	var hint *RetryAfterError
	if errors.As(err, &hint) && hint.After > left {
		// The provider asks for a longer wait than the step has left; the
		// next run must come back in time to release the hold.
		err = &RetryAfterError{After: left, Err: hint.Err}
	}
	if err != nil {
		return stepError(transfers.StepScreen, err)
	}
	switch res.Decision {
	case DecisionApprove:
		return s.finishStep(ctx, id, transfers.StepScreen, transfers.StatusHeld,
			func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
				tr.ScreeningID = res.ID
				if _, err := tr.FinishStep(transfers.StepScreen, transfers.StepSucceeded, transfers.OutcomeApprove, now); err != nil {
					return nil, err
				}
				next := s.step(KindCapture, tr.ID, nil)
				return &next, nil
			})
	case DecisionDecline:
		return s.compensate(ctx, id, transfers.StepScreen, transfers.StepSucceeded, transfers.OutcomeDecline,
			transfers.StatusRejected, transfers.ReasonScreeningDeclined)
	case DecisionReview:
		err := s.finishStep(ctx, id, transfers.StepScreen, transfers.StatusHeld,
			func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
				tr.ScreeningID = res.ID
				_, err := tr.FinishStep(transfers.StepScreen, transfers.StepWaiting, transfers.OutcomeReview, now)
				return nil, err
			})
		if err != nil {
			return err
		}
		after := min(max(res.RetryAfter, s.cfg.ScreenPollMin), s.cfg.ScreenPollMax, left)
		return taskqueue.RetryLater(after, "screening under review")
	default:
		return taskqueue.Permanent(fmt.Errorf("screen: unknown decision %q", res.Decision))
	}
}

// capture moves the money: the point of no return. After an approval the
// saga only goes forward, retrying with the same key until the ledger
// answers; a hold that expired in the meantime is released instead.
func (s *Service) capture(ctx context.Context, id uuid.UUID) error {
	tr, ok, err := s.beginStep(ctx, id, transfers.StepCapture, transfers.StatusHeld)
	if err != nil || !ok {
		return err
	}
	if rec, _ := tr.Step(transfers.StepScreen); rec.Outcome != transfers.OutcomeApprove {
		return taskqueue.Permanent(fmt.Errorf("capture of transfer %s without an approved screening", id))
	}
	c, err := s.ledger.CaptureHold(ctx, tr.HoldID, transfers.StepKey(id, transfers.StepCapture))
	if errors.Is(err, ErrHoldExpired) {
		return s.compensate(ctx, id, transfers.StepCapture, transfers.StepFailed, transfers.OutcomeHoldExpired,
			transfers.StatusCanceled, transfers.ReasonHoldExpired)
	}
	if err != nil {
		return stepError(transfers.StepCapture, err)
	}
	return s.finishStep(ctx, id, transfers.StepCapture, transfers.StatusHeld,
		func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
			tr.LedgerTransactionID = c.TransactionID
			if _, err := tr.FinishStep(transfers.StepCapture, transfers.StepSucceeded, transfers.OutcomeCaptured, now); err != nil {
				return nil, err
			}
			return nil, tr.TransitionTo(transfers.StatusCompleted, "")
		})
}

// compensate finishes step with outcome and schedules the release of the
// hold, after which the transfer ends with status and reason.
func (s *Service) compensate(ctx context.Context, id uuid.UUID, step transfers.Step, state transfers.StepState,
	outcome string, status transfers.Status, reason transfers.Reason) error {
	payload, err := json.Marshal(releasePayload{Status: status, Reason: reason})
	if err != nil {
		return taskqueue.Permanent(err)
	}
	return s.finishStep(ctx, id, step, transfers.StatusHeld,
		func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
			if _, err := tr.FinishStep(step, state, outcome, now); err != nil {
				return nil, err
			}
			next := s.step(KindRelease, tr.ID, payload)
			return &next, nil
		})
}

// release gives the held money back and ends the transfer. The ledger
// treats a released hold as a replay, so this is safe to repeat and safe
// after the ledger's expiry sweep has released the hold already. A captured
// transfer never gets here: the capture's commit moves it to completed, and
// beginStep wants held. The ledger refuses to release a captured hold all
// the same.
func (s *Service) release(ctx context.Context, id uuid.UUID, payload json.RawMessage) error {
	var p releasePayload
	if err := json.Unmarshal(payload, &p); err != nil || !p.Status.Final() || p.Status == transfers.StatusCompleted {
		return taskqueue.Permanent(fmt.Errorf("release: bad payload %s", payload))
	}
	tr, ok, err := s.beginStep(ctx, id, transfers.StepRelease, transfers.StatusHeld)
	if err != nil || !ok {
		return err
	}
	if _, err := s.ledger.ReleaseHold(ctx, tr.HoldID, transfers.StepKey(id, transfers.StepRelease), string(p.Reason)); err != nil {
		if errors.Is(err, ErrHoldCaptured) {
			s.log.ErrorContext(ctx, "ledger reports the hold captured, but the saga decided to release it", "transfer", tr)
		}
		return stepError(transfers.StepRelease, err)
	}
	return s.finishStep(ctx, id, transfers.StepRelease, transfers.StatusHeld,
		func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error) {
			if _, err := tr.FinishStep(transfers.StepRelease, transfers.StepSucceeded, transfers.OutcomeReleased, now); err != nil {
				return nil, err
			}
			return nil, tr.TransitionTo(p.Status, p.Reason)
		})
}

// stepError classifies a failed remote call for the task queue: an answer
// that may change on a retry with the same key is retried, a contract
// violation is permanent.
func stepError(step transfers.Step, err error) error {
	if errors.Is(err, ErrLedgerUnavailable) || errors.Is(err, ErrScreeningUnavailable) {
		return fmt.Errorf("%s: %w", step, err)
	}
	return taskqueue.Permanent(fmt.Errorf("%s: %w", step, err))
}

// beginStep records an attempt of step in its own short transaction,
// before the remote call: after a crash, a step left running says that a
// call may have happened. It returns the transfer as committed, and
// ok=false when the saga is past this step, which makes the task obsolete.
func (s *Service) beginStep(ctx context.Context, id uuid.UUID, step transfers.Step, want transfers.Status) (transfers.Transfer, bool, error) {
	var (
		tr transfers.Transfer
		ok bool
	)
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		cur, err := s.transfers.Lock(ctx, id)
		if errors.Is(err, transfers.ErrNotFound) {
			return taskqueue.Permanent(err)
		}
		if err != nil {
			return fmt.Errorf("lock transfer: %w", err)
		}
		if rec, found := cur.Step(step); cur.Status != want || (found && rec.Done()) {
			return nil
		}
		prev := cur.Version
		now := s.clock.Now()
		if cur.StartStep(step, now) {
			cur.Touch(now)
		}
		if err := s.transfers.Update(ctx, cur, prev); err != nil {
			return fmt.Errorf("record %s attempt: %w", step, err)
		}
		tr, ok = cur, true
		return nil
	})
	return tr, ok, err
}

// stepResult is what finishStep committed, reported afterwards.
type stepResult struct {
	transfer transfers.Transfer
	step     transfers.Step
	outcome  string
	from     transfers.Status
}

// finishStep applies the answer of a step's remote call in one
// transaction: the step's outcome, the status, the event of a status change
// and the next step's task. It checks again, under the row lock, that the
// step is still open: a worker whose lease expired may have finished it
// already, and then this one does nothing.
//
// apply changes the transfer and returns the next task, if any. An error
// from it is a bug (the checks above make every transition legal), so it
// is permanent.
func (s *Service) finishStep(ctx context.Context, id uuid.UUID, step transfers.Step, want transfers.Status,
	apply func(tr *transfers.Transfer, now time.Time) (*taskqueue.Spec, error)) error {
	var res *stepResult
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		tr, err := s.transfers.Lock(ctx, id)
		if err != nil {
			return fmt.Errorf("lock transfer: %w", err)
		}
		before, found := tr.Step(step)
		if tr.Status != want || !found || before.Done() {
			return nil
		}
		prev, from := tr.Version, tr.Status
		now := s.clock.Now()
		next, err := apply(&tr, now)
		if err != nil {
			return taskqueue.Permanent(fmt.Errorf("apply %s result: %w", step, err))
		}
		after, _ := tr.Step(step)
		if after == before && tr.Status == from {
			return nil // a repeated review: nothing to record
		}
		tr.Touch(now)
		if err := s.transfers.Update(ctx, tr, prev); err != nil {
			return fmt.Errorf("save %s result: %w", step, err)
		}
		if tr.Status != from {
			eventID, err := s.id()
			if err != nil {
				return err
			}
			if err := s.events.Record(ctx, transfers.NewEvent(eventID, tr)); err != nil {
				return fmt.Errorf("record event: %w", err)
			}
		}
		if next != nil {
			if _, err := s.tasks.Enqueue(ctx, *next); err != nil {
				return fmt.Errorf("enqueue %s: %w", next.Kind, err)
			}
		}
		res = &stepResult{transfer: tr, step: step, outcome: after.Outcome, from: from}
		return nil
	})
	if err != nil || res == nil {
		return err
	}
	// Counted and logged only after the commit: a rolled-back change never
	// shows up in the metrics.
	s.metrics.StepFinished(res.step, res.outcome)
	if tr := res.transfer; tr.Status != res.from {
		s.metrics.StatusChanged(res.from, tr.Status, tr.UpdatedAt.Sub(tr.CreatedAt))
		s.log.InfoContext(ctx, "transfer status changed", "transfer", tr, "from", res.from, "reason", tr.Reason)
	}
	return nil
}
