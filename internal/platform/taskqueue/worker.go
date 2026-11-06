package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// settleTimeout bounds the statement that records a task's outcome.
const settleTimeout = 5 * time.Second

// errNeverSettled buries a task whose last run never recorded an outcome.
var errNeverSettled = errors.New("out of attempts: the last run never settled, its process may have crashed in the handler")

// Claimer leases due tasks.
type Claimer interface {
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]Task, error)
}

// Settler records the outcome of a claimed task. Every method returns
// ErrLeaseLost if the lease has passed to another worker.
type Settler interface {
	Complete(ctx context.Context, t Task) error
	Retry(ctx context.Context, t Task, runAfter time.Time, cause string) error
	Postpone(ctx context.Context, t Task, runAfter time.Time) error
	Bury(ctx context.Context, t Task, cause string) error
}

// Store is what the Worker needs from the task table.
type Store interface {
	Claimer
	Settler
}

// Handler executes one task.
type Handler interface {
	HandleTask(ctx context.Context, t Task) error
}

// Clock returns the current time. Leases and run_after use it, so tests can
// expire a lease without sleeping.
type Clock interface {
	Now() time.Time
}

// Config tunes a Worker.
type Config struct {
	// Concurrency is the number of tasks run at once.
	Concurrency int
	// IdleInterval is how often an idle worker looks for due tasks.
	IdleInterval time.Duration
	// Lease is how long a claimed task stays invisible to other workers.
	Lease time.Duration
	// TaskTimeout bounds one handler run. It must be shorter than Lease, so
	// a live worker settles its task before the lease can pass to another.
	TaskTimeout time.Duration
	// MaxAttempts is the number of failed runs after which a task is dead.
	// A run whose process died before it settled counts as failed.
	MaxAttempts int
	Backoff     Backoff
}

// Worker claims and runs tasks.
type Worker struct {
	store     Store
	handler   Handler
	clock     Clock
	wake      <-chan struct{}
	metrics   *Metrics
	log       *slog.Logger
	cfg       Config
	stopClaim chan struct{}
	stopOnce  sync.Once
}

// NewWorker returns a Worker. wake may be nil; when set, a value on it
// starts a claim immediately instead of at the next idle tick.
func NewWorker(store Store, handler Handler, clock Clock, wake <-chan struct{}, m *Metrics, log *slog.Logger, cfg Config) *Worker {
	return &Worker{
		store:     store,
		handler:   handler,
		clock:     clock,
		wake:      wake,
		metrics:   m,
		log:       log,
		cfg:       cfg,
		stopClaim: make(chan struct{}),
	}
}

// StopClaiming makes Run take no new tasks. Tasks already running finish;
// Run itself returns when its context is cancelled. Shutdown calls it first,
// so the worker starts no new remote calls while HTTP drains.
func (w *Worker) StopClaiming() {
	w.stopOnce.Do(func() { close(w.stopClaim) })
}

// Run keeps up to Concurrency tasks running: it claims as many tasks as
// there are free slots, and claims again as soon as a slot frees, a commit
// hook signals new work, or the idle tick fires. A slow task therefore
// blocks only its own slot, not the claim of the next batch.
//
// Run returns after ctx is cancelled and every running task is settled.
// Running tasks are never cancelled by it (see process).
func (w *Worker) Run(ctx context.Context) error {
	var running sync.WaitGroup
	defer running.Wait()
	// Each finished task sends one value; there are never more than
	// Concurrency of them pending, so the send never blocks.
	freed := make(chan struct{}, w.cfg.Concurrency)
	busy := 0
	stopClaim := w.stopClaim
	stopped := false
	ticker := time.NewTicker(w.cfg.IdleInterval)
	defer ticker.Stop()

	claim := true
	for {
		if !stopped {
			select {
			case <-stopClaim:
				stopped, stopClaim = true, nil
			default:
			}
		}
		if claim && !stopped {
			if free := w.cfg.Concurrency - busy; free > 0 {
				now := w.clock.Now()
				tasks, err := w.store.Claim(ctx, now, free, w.cfg.Lease)
				if ctx.Err() != nil {
					w.giveBack(ctx, tasks)
					return nil
				}
				if err != nil {
					w.log.ErrorContext(ctx, "claim tasks failed", "err", err)
				}
				for _, t := range tasks {
					w.observeClaim(t, now)
					busy++
					running.Add(1)
					go func() {
						defer running.Done()
						w.process(ctx, t)
						freed <- struct{}{}
					}()
				}
				if err == nil && len(tasks) == free {
					continue // every slot filled: more work is probably waiting
				}
			}
		}
		claim = false
		select {
		case <-ctx.Done():
			return nil
		case <-stopClaim:
			stopped, stopClaim = true, nil
		case <-ticker.C:
			claim = true
		case <-w.wake:
			claim = true
		case <-freed:
			busy--
			claim = true
		}
	}
}

// RunOnce claims one batch of up to Concurrency tasks, runs it and waits
// until every task is settled. It returns the number of tasks claimed.
// Tests and tools use it to step the queue deterministically.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock.Now()
	tasks, err := w.store.Claim(ctx, now, w.cfg.Concurrency, w.cfg.Lease)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, t := range tasks {
		w.observeClaim(t, now)
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.process(ctx, t)
		}()
	}
	wg.Wait()
	return len(tasks), nil
}

// giveBack returns tasks claimed but never started, so they do not wait out
// their lease and do not spend an attempt.
func (w *Worker) giveBack(ctx context.Context, tasks []Task) {
	if len(tasks) == 0 {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	for _, t := range tasks {
		if err := w.store.Postpone(sctx, t, t.RunAfter); err != nil {
			w.log.WarnContext(ctx, "give back an unstarted task failed; it runs when the lease expires",
				"task_id", t.ID, "kind", t.Kind, "err", err)
		}
	}
}

func (w *Worker) observeClaim(t Task, now time.Time) {
	w.metrics.claimed.WithLabelValues(t.Kind).Inc()
	w.metrics.lag.WithLabelValues(t.Kind).Observe(max(now.Sub(t.RunAfter), 0).Seconds())
}

// process runs one task and records the outcome.
//
// A task claimed with its attempts already spent had its last run end
// without a settle: the process died in the handler. It is buried without
// running, or a handler that crashes its process would loop forever.
//
// The handler gets a context detached from the worker's cancellation and
// bounded by TaskTimeout. On shutdown we stop claiming but let a running
// task finish: cancelling it in the middle of a remote call would turn a
// clean result into an ambiguous one.
func (w *Worker) process(ctx context.Context, t Task) {
	if t.Attempts >= w.cfg.MaxAttempts {
		w.settle(ctx, t, errNeverSettled)
		return
	}
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.TaskTimeout)
	defer cancel()
	start := time.Now()
	err := w.handle(hctx, t)
	w.metrics.duration.WithLabelValues(t.Kind).Observe(time.Since(start).Seconds())
	w.settle(ctx, t, err)
}

func (w *Worker) handle(ctx context.Context, t Task) (err error) {
	defer func() {
		if p := recover(); p != nil {
			w.metrics.panics.Inc()
			w.log.ErrorContext(ctx, "task handler panicked",
				"task_id", t.ID, "kind", t.Kind, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return w.handler.HandleTask(ctx, t)
}

func (w *Worker) settle(ctx context.Context, t Task, handleErr error) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	now := w.clock.Now()
	attrs := []any{"task_id", t.ID, "kind", t.Kind, "aggregate_id", t.AggregateID, "attempt", t.Attempts + 1}

	var (
		later   *RetryLaterError
		perm    *PermanentError
		hint    retryAfterer
		err     error
		outcome string
	)
	switch {
	case handleErr == nil:
		err, outcome = w.store.Complete(sctx, t), outcomeOK
	case errors.As(handleErr, &later):
		// The run worked; the task simply runs again later (polling).
		err, outcome = w.store.Postpone(sctx, t, now.Add(later.After)), outcomeOK
	case errors.As(handleErr, &perm), t.Attempts+1 >= w.cfg.MaxAttempts:
		outcome = outcomeDead
		if perm != nil {
			outcome = outcomePermanent
		}
		err = w.store.Bury(sctx, t, handleErr.Error())
		if err == nil {
			w.log.ErrorContext(ctx, "task moved to dead state", append(attrs, "err", handleErr)...)
		}
	default:
		delay := w.cfg.Backoff.Delay(t.Attempts + 1)
		if errors.As(handleErr, &hint) {
			// The downstream system asked us to wait; never come back sooner.
			delay = max(delay, hint.RetryAfter())
		}
		err, outcome = w.store.Retry(sctx, t, now.Add(delay), handleErr.Error()), outcomeRetry
		if err == nil {
			w.log.WarnContext(ctx, "task failed, will retry", append(attrs, "err", handleErr, "retry_in", delay.String())...)
		}
	}

	// An outcome is counted only once it is recorded: a lost lease or a
	// failed settle means the task will run again and be counted then.
	switch {
	case errors.Is(err, ErrLeaseLost):
		w.metrics.leaseLost.Inc()
		w.log.WarnContext(ctx, "task lease lost; result discarded", attrs...)
	case err != nil:
		w.log.ErrorContext(ctx, "settle task failed; it runs again when the lease expires", append(attrs, "err", err)...)
	default:
		w.metrics.attempts.WithLabelValues(t.Kind, outcome).Inc()
	}
}
