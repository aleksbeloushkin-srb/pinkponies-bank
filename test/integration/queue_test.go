//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

const kindTest = "test"

type handlerFunc func(ctx context.Context, t taskqueue.Task) error

func (f handlerFunc) HandleTask(ctx context.Context, t taskqueue.Task) error { return f(ctx, t) }

func queueConfig() taskqueue.Config {
	return taskqueue.Config{
		Concurrency: 5, IdleInterval: time.Hour, Lease: time.Minute, TaskTimeout: 10 * time.Second, MaxAttempts: 3,
		Backoff: taskqueue.Backoff{Base: time.Second, Max: 10 * time.Second},
	}
}

// newQueue resets the task table of the transfers database and returns the
// queue on it.
func newQueue(t *testing.T) (*taskqueue.Postgres, *clock) {
	t.Helper()
	if _, err := transfersDB.Exec(context.Background(), `TRUNCATE tasks RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
	return taskqueue.NewPostgres(txctx.New(transfersDB)), newClock()
}

func enqueue(t *testing.T, q *taskqueue.Postgres, c *clock, n int) {
	t.Helper()
	for range n {
		if _, err := q.Enqueue(context.Background(), taskqueue.Spec{
			Kind: kindTest, AggregateID: uuid.New(), DedupKey: "1", RunAfter: c.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func newQueueWorker(q *taskqueue.Postgres, h taskqueue.Handler, c *clock) *taskqueue.Worker {
	return taskqueue.NewWorker(q, h, c, nil, taskqueue.NewMetrics(prometheus.NewRegistry(), []string{kindTest}), discard(), queueConfig())
}

// Eight workers drain the same table at once. SKIP LOCKED and the lease
// guarantee that each task is handled exactly once.
func TestQueueConcurrentWorkersNeverDoubleProcess(t *testing.T) {
	q, c := newQueue(t)
	const tasks, workers = 200, 8
	enqueue(t, q, c, tasks)

	var mu sync.Mutex
	seen := map[int64]int{}
	h := handlerFunc(func(_ context.Context, task taskqueue.Task) error {
		mu.Lock()
		seen[task.ID]++
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := newQueueWorker(q, h, c)
			for {
				n, err := w.RunOnce(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != tasks {
		t.Fatalf("handled %d distinct tasks, want %d", len(seen), tasks)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("task %d handled %d times", id, n)
		}
	}
	if n := count(t, transfersDB, `SELECT count(*) FROM tasks WHERE state = 'done'`); n != tasks {
		t.Fatalf("done = %d, want %d", n, tasks)
	}
}

// A worker that dies keeps its lease until it expires; then another worker
// claims the task, and the first one can no longer settle it.
func TestQueueLeaseExpiryAndFencing(t *testing.T) {
	q, c := newQueue(t)
	ctx := context.Background()
	enqueue(t, q, c, 1)
	t0 := c.Now()

	first, err := q.Claim(ctx, t0, 10, 45*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %v, %v", first, err)
	}
	if again, _ := q.Claim(ctx, t0.Add(10*time.Second), 10, 45*time.Second); len(again) != 0 {
		t.Fatalf("claimed %d tasks while the lease is held", len(again))
	}
	second, err := q.Claim(ctx, t0.Add(46*time.Second), 10, 45*time.Second)
	if err != nil || len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatalf("claim after expiry = %v, %v", second, err)
	}
	if err := q.Complete(ctx, first[0]); !errors.Is(err, taskqueue.ErrLeaseLost) {
		t.Fatalf("stale owner Complete = %v, want ErrLeaseLost", err)
	}
	if err := q.Complete(ctx, second[0]); err != nil {
		t.Fatalf("current owner Complete = %v", err)
	}
}

// A task that keeps failing is retried with backoff and goes dead after
// MaxAttempts; a postponed one spends no attempts.
func TestQueueDeadAndPostponed(t *testing.T) {
	q, c := newQueue(t)
	enqueue(t, q, c, 1)
	var runs atomic.Int32
	w := newQueueWorker(q, handlerFunc(func(context.Context, taskqueue.Task) error {
		runs.Add(1)
		return errors.New("ledger down")
	}), c)
	for range queueConfig().MaxAttempts {
		if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunOnce = %d, %v", n, err)
		}
		if n, _ := w.RunOnce(context.Background()); n != 0 {
			t.Fatal("a failed task must wait for its backoff")
		}
		c.Advance(time.Minute)
	}
	if n, _ := w.RunOnce(context.Background()); n != 0 {
		t.Fatal("a dead task must never be claimed again")
	}
	var state, lastError string
	var attempts int
	if err := transfersDB.QueryRow(context.Background(), `SELECT state, attempts, last_error FROM tasks`).Scan(&state, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if state != "dead" || attempts != 3 || lastError != "ledger down" || runs.Load() != 3 {
		t.Fatalf("state=%s attempts=%d last_error=%q runs=%d", state, attempts, lastError, runs.Load())
	}

	q, c = newQueue(t)
	enqueue(t, q, c, 1)
	w = newQueueWorker(q, handlerFunc(func(context.Context, taskqueue.Task) error {
		return taskqueue.RetryLater(5*time.Second, "review")
	}), c)
	for range 5 {
		if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunOnce = %d, %v", n, err)
		}
		c.Advance(5 * time.Second)
	}
	if n := count(t, transfersDB, `SELECT attempts FROM tasks`); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
}

// A handler that kills its process never settles its task. The claim counts
// the attempt anyway, so after MaxAttempts such runs the next claim buries
// the task without running it, instead of crashing the process forever.
func TestQueueCrashLoopGoesDead(t *testing.T) {
	q, c := newQueue(t)
	enqueue(t, q, c, 1)
	cfg := queueConfig()
	for i := range cfg.MaxAttempts {
		tasks, err := q.Claim(context.Background(), c.Now(), 1, cfg.Lease)
		if err != nil || len(tasks) != 1 || tasks[0].Attempts != i {
			t.Fatalf("claim %d = %+v, %v", i+1, tasks, err)
		}
		// The process dies here: nothing is settled.
		c.Advance(cfg.Lease + time.Second)
	}
	var runs atomic.Int32
	w := newQueueWorker(q, handlerFunc(func(context.Context, taskqueue.Task) error {
		runs.Add(1)
		return nil
	}), c)
	if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	var (
		state    string
		attempts int
	)
	if err := transfersDB.QueryRow(context.Background(), `SELECT state, attempts FROM tasks`).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if state != "dead" || runs.Load() != 0 {
		t.Fatalf("after %d runs that never settled: state=%s attempts=%d runs=%d, want dead without running",
			cfg.MaxAttempts, state, attempts, runs.Load())
	}
}

// Enqueueing is idempotent by (kind, aggregate, dedup key), and a task
// enqueued in a rolled-back transaction does not exist.
func TestQueueEnqueueIsTransactional(t *testing.T) {
	q, c := newQueue(t)
	tx := txctx.New(transfersDB)
	spec := taskqueue.Spec{Kind: kindTest, AggregateID: uuid.New(), DedupKey: "saga", RunAfter: c.Now()}
	if ok, err := q.Enqueue(context.Background(), spec); err != nil || !ok {
		t.Fatalf("first enqueue = %v, %v", ok, err)
	}
	if ok, err := q.Enqueue(context.Background(), spec); err != nil || ok {
		t.Fatalf("second enqueue = %v, %v; want ignored", ok, err)
	}
	boom := errors.New("boom")
	err := tx.Run(context.Background(), func(ctx context.Context) error {
		if _, err := q.Enqueue(ctx, taskqueue.Spec{Kind: kindTest, AggregateID: uuid.New(), DedupKey: "saga", RunAfter: c.Now()}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if n := count(t, transfersDB, `SELECT count(*) FROM tasks`); n != 1 {
		t.Fatalf("tasks = %d, want 1", n)
	}
}
