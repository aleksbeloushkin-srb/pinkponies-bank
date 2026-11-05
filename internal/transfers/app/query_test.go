package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

func (f *fixture) pending(t *testing.T) transfers.Transfer {
	t.Helper()
	res, err := f.svc.CreateTransfer(context.Background(), f.request("k", 500))
	if err != nil {
		t.Fatal(err)
	}
	return res.Transfer
}

func TestWaitAnswersAtOnceWhenAlreadyNewer(t *testing.T) {
	f := newFixture(t)
	tr := f.pending(t)
	res, err := f.svc.WaitTransfer(context.Background(), tr.ID, 0)
	if err != nil || !res.Modified || res.Transfer.Version != 1 {
		t.Fatalf("WaitTransfer = %+v, %v", res, err)
	}
}

// The waiter is woken by the watcher and reads the new state.
func TestWaitWakesOnChange(t *testing.T) {
	f := newFixture(t)
	tr := f.pending(t)
	f.svc.cfg.WaitTimeout = time.Hour // only the change can end this wait
	f.watcher.watched = make(chan uuid.UUID, 1)

	done := make(chan WaitResult)
	go func() {
		res, err := f.svc.WaitTransfer(context.Background(), tr.ID, 1)
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	// Let the saga move the transfer, then fire the waiter.
	if _, err := f.worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-f.watcher.watched // the waiter is registered
	f.watcher.fire(tr.ID, 2)
	res := <-done
	if !res.Modified || res.Transfer.Version <= 1 {
		t.Fatalf("WaitTransfer = %+v", res)
	}
}

func TestWaitTimesOutUnmodified(t *testing.T) {
	f := newFixture(t)
	tr := f.pending(t)
	f.svc.cfg.WaitTimeout = time.Millisecond
	res, err := f.svc.WaitTransfer(context.Background(), tr.ID, 1)
	if err != nil || res.Modified || res.Transfer.Version != 1 {
		t.Fatalf("WaitTransfer = %+v, %v", res, err)
	}
}

func TestWaitClientGone(t *testing.T) {
	f := newFixture(t)
	tr := f.pending(t)
	f.svc.cfg.WaitTimeout = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.svc.WaitTransfer(ctx, tr.ID, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestWaitValidatesAndPropagates(t *testing.T) {
	f := newFixture(t)
	tr := f.pending(t)
	var verr *transfers.ValidationError
	if _, err := f.svc.WaitTransfer(context.Background(), tr.ID, -1); !errors.As(err, &verr) {
		t.Fatalf("negative version: %v", err)
	}
	full := errors.New("too many waiters")
	f.watcher.err = full
	if _, err := f.svc.WaitTransfer(context.Background(), tr.ID, 1); !errors.Is(err, full) {
		t.Fatalf("watcher error: %v", err)
	}
	f.watcher.err = nil
	if _, err := f.svc.WaitTransfer(context.Background(), uuid.New(), 0); !errors.Is(err, transfers.ErrNotFound) {
		t.Fatalf("unknown transfer: %v", err)
	}
}
