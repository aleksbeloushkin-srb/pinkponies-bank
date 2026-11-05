package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// Transfer returns the current state of a transfer.
func (s *Service) Transfer(ctx context.Context, id uuid.UUID) (transfers.Transfer, error) {
	return s.transfers.Transfer(ctx, id)
}

// WaitResult is the outcome of a long poll. Modified is false when the soft
// timeout expired (or the instance is shutting down) before the version
// moved past the client's marker; Transfer then holds the last state read.
type WaitResult struct {
	Transfer transfers.Transfer
	Modified bool
}

// WaitTransfer blocks until the transfer's version exceeds seenVersion, the
// soft timeout expires, or ctx is cancelled because the client went away.
//
// The waiter is registered before the first read: a full registry answers
// 429 without touching the database, and a change that lands between the
// read and the wait is still caught, because the batcher compares versions
// instead of relying on being told.
func (s *Service) WaitTransfer(ctx context.Context, id uuid.UUID, seenVersion int64) (WaitResult, error) {
	if seenVersion < 0 {
		return WaitResult{}, &transfers.ValidationError{Field: "version", Reason: "must not be negative"}
	}
	changes, cancel, err := s.watcher.Watch(id, seenVersion)
	if err != nil {
		return WaitResult{}, err
	}
	defer cancel()

	t, err := s.readForWait(ctx, id)
	if err != nil {
		return WaitResult{}, err
	}
	if t.Version > seenVersion {
		return WaitResult{Transfer: t, Modified: true}, nil
	}

	// A timer, not a derived context: the two ways out must stay apart. Our
	// own timeout is a normal "not modified" answer; ctx.Done means the
	// client has gone and nobody will read the response.
	timer := time.NewTimer(s.cfg.WaitTimeout)
	defer timer.Stop()
	select {
	case _, ok := <-changes:
		if !ok {
			return WaitResult{Transfer: t}, nil
		}
	case <-timer.C:
		return WaitResult{Transfer: t}, nil
	case <-ctx.Done():
		return WaitResult{}, ctx.Err()
	}

	t, err = s.readForWait(ctx, id)
	if err != nil {
		return WaitResult{}, err
	}
	return WaitResult{Transfer: t, Modified: true}, nil
}

func (s *Service) readForWait(ctx context.Context, id uuid.UUID) (transfers.Transfer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.WaitReadTimeout)
	defer cancel()
	return s.transfers.Transfer(ctx, id)
}

// Accounts lists the customer accounts, read from the ledger.
func (s *Service) Accounts(ctx context.Context) ([]Account, error) {
	return s.ledger.Accounts(ctx)
}
