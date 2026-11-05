package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// ReleaseExpiredHolds releases up to limit active holds whose expiry has
// passed and returns how many it released. Each hold is released in its own
// short transaction. A hold whose row another transaction holds (a capture
// or a release in progress) is skipped rather than waited for; the next
// sweep finds it again if it is still active and expired.
func (s *Service) ReleaseExpiredHolds(ctx context.Context, limit int) (int, error) {
	ids, err := s.holds.ExpiredHolds(ctx, s.clock.Now(), limit)
	if err != nil {
		return 0, fmt.Errorf("list expired holds: %w", err)
	}
	released := 0
	for _, id := range ids {
		ok, err := s.releaseExpired(ctx, id)
		if err != nil {
			return released, fmt.Errorf("release expired hold %s: %w", id, err)
		}
		if ok {
			released++
		}
	}
	return released, nil
}

func (s *Service) releaseExpired(ctx context.Context, id uuid.UUID) (bool, error) {
	res, err := s.release(ctx, id, true, func(h *ledger.Hold, now time.Time) (bool, error) {
		// Captured, released or extended since the list was read: nothing to
		// do. Only an active, expired hold is released.
		if h.Status != ledger.HoldActive || !h.Expired(now) {
			return false, nil
		}
		return h.Release(ledger.ReasonExpired, "", now)
	})
	if errors.Is(err, errHoldBusy) {
		return false, nil
	}
	if err != nil || res.Replayed {
		return false, err
	}
	s.metrics.HoldReleased(CauseExpired)
	s.log.InfoContext(ctx, "expired hold released", "hold", res.Hold)
	return true, nil
}

// Sweeper releases expired holds on a fixed interval. It is a background
// component: Run returns when ctx is cancelled.
type Sweeper struct {
	svc      *Service
	interval time.Duration
	batch    int
	log      *slog.Logger
}

// NewSweeper returns a Sweeper that releases up to batch holds per tick.
func NewSweeper(svc *Service, interval time.Duration, batch int, log *slog.Logger) *Sweeper {
	return &Sweeper{svc: svc, interval: interval, batch: batch, log: log}
}

// Run sweeps until ctx is cancelled. A full batch is followed by another one
// at once; a failed sweep is logged and retried on the next tick.
func (w *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		n, err := w.svc.ReleaseExpiredHolds(ctx, w.batch)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			w.log.WarnContext(ctx, "hold expiry sweep failed", "err", err)
		}
		if err == nil && n == w.batch {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
