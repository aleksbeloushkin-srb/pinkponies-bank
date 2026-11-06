package taskqueue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

const kindTest = "test"

type settled struct {
	how      string
	taskID   int64
	runAfter time.Time
	cause    string
}

// fakeStore hands out queued tasks once and records how they were settled.
type fakeStore struct {
	mu        sync.Mutex
	queue     []Task
	settled   []settled
	settleErr error
	claims    chan int // receives the limit of every Claim when set
	onClaim   func()   // runs inside every Claim when set
}

func (s *fakeStore) Claim(_ context.Context, _ time.Time, limit int, _ time.Duration) ([]Task, error) {
	if s.claims != nil {
		s.claims <- limit
	}
	if s.onClaim != nil {
		s.onClaim()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.queue))
	out := s.queue[:n]
	s.queue = s.queue[n:]
	return out, nil
}

func (s *fakeStore) record(st settled) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled = append(s.settled, st)
	return s.settleErr
}

func (s *fakeStore) Complete(_ context.Context, t Task) error {
	return s.record(settled{how: "done", taskID: t.ID})
}

func (s *fakeStore) Retry(_ context.Context, t Task, runAfter time.Time, cause string) error {
	return s.record(settled{how: "retried", taskID: t.ID, runAfter: runAfter, cause: cause})
}

func (s *fakeStore) Postpone(_ context.Context, t Task, runAfter time.Time) error {
	return s.record(settled{how: "postponed", taskID: t.ID, runAfter: runAfter})
}

func (s *fakeStore) Bury(_ context.Context, t Task, cause string) error {
	return s.record(settled{how: "dead", taskID: t.ID, cause: cause})
}

func (s *fakeStore) settledCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.settled)
}

type handlerFunc func(ctx context.Context, t Task) error

func (f handlerFunc) HandleTask(ctx context.Context, t Task) error { return f(ctx, t) }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var (
	t0      = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	testCfg = Config{
		Concurrency:  4,
		IdleInterval: time.Hour,
		Lease:        time.Minute,
		TaskTimeout:  time.Second,
		MaxAttempts:  3,
		Backoff:      Backoff{Base: time.Second, Max: time.Minute, Rand: func(int64) int64 { return 0 }},
	}
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newWorker(store *fakeStore, h Handler, wake <-chan struct{}, cfg Config) *Worker {
	return NewWorker(store, h, fixedClock{t0}, wake, NewMetrics(prometheus.NewRegistry(), []string{kindTest}), discard(), cfg)
}

func task(id int64, attempts int) Task {
	return Task{ID: id, Kind: kindTest, AggregateID: uuid.New(), Attempts: attempts}
}

// hinted is a failure with a downstream "retry after" hint.
type hinted struct{ after time.Duration }

func (e hinted) Error() string             { return "rate limited" }
func (e hinted) RetryAfter() time.Duration { return e.after }

func TestWorkerSettlesEachOutcome(t *testing.T) {
	boom := errors.New("ledger down")
	tests := []struct {
		name     string
		attempts int
		err      error
		want     settled
		outcome  string
	}{
		{"success", 0, nil, settled{how: "done"}, outcomeOK},
		{"first failure backs off", 0, boom, settled{how: "retried", runAfter: t0.Add(500 * time.Millisecond), cause: boom.Error()}, outcomeRetry},
		{"second failure backs off longer", 1, boom, settled{how: "retried", runAfter: t0.Add(time.Second), cause: boom.Error()}, outcomeRetry},
		{"last attempt goes dead", 2, boom, settled{how: "dead", cause: boom.Error()}, outcomeDead},
		{"permanent goes dead at once", 0, Permanent(boom), settled{how: "dead", cause: "permanent: " + boom.Error()}, outcomePermanent},
		{"retry later postpones", 0, RetryLater(5*time.Second, "review"), settled{how: "postponed", runAfter: t0.Add(5 * time.Second)}, outcomeOK},
		{"hint longer than backoff wins", 0, hinted{time.Minute}, settled{how: "retried", runAfter: t0.Add(time.Minute), cause: "rate limited"}, outcomeRetry},
		{"backoff longer than hint wins", 1, hinted{time.Millisecond}, settled{how: "retried", runAfter: t0.Add(time.Second), cause: "rate limited"}, outcomeRetry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{queue: []Task{task(7, tt.attempts)}}
			w := newWorker(store, handlerFunc(func(context.Context, Task) error { return tt.err }), nil, testCfg)

			n, err := w.RunOnce(context.Background())

			if err != nil || n != 1 {
				t.Fatalf("RunOnce = %d, %v", n, err)
			}
			tt.want.taskID = 7
			if len(store.settled) != 1 || store.settled[0] != tt.want {
				t.Fatalf("settled = %+v, want %+v", store.settled, tt.want)
			}
			if got := testutil.ToFloat64(w.metrics.attempts.WithLabelValues(kindTest, tt.outcome)); got != 1 {
				t.Fatalf("task_attempts_total{outcome=%q} = %v, want 1", tt.outcome, got)
			}
			if total := attemptsTotal(w.metrics); total != 1 {
				t.Fatalf("attempts recorded = %v, want exactly one", total)
			}
		})
	}
}

func attemptsTotal(m *Metrics) float64 {
	var total float64
	for _, o := range outcomes {
		total += testutil.ToFloat64(m.attempts.WithLabelValues(kindTest, o))
	}
	return total
}

func TestWorkerRecoversHandlerPanic(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, 0)}}
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { panic("nil map") }), nil, testCfg)

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.settled[0]; got.how != "retried" || got.cause != "handler panic: nil map" {
		t.Fatalf("settled = %+v, want a retry", got)
	}
	if got := testutil.ToFloat64(w.metrics.panics); got != 1 {
		t.Fatalf("task_panics_total = %v, want 1", got)
	}
}

func TestWorkerSurvivesLostLease(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, 0)}, settleErr: ErrLeaseLost}
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { return nil }), nil, testCfg)

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("a lost lease is not a worker error: %v", err)
	}
	if got := testutil.ToFloat64(w.metrics.leaseLost); got != 1 {
		t.Fatalf("lease_lost_total = %v, want 1", got)
	}
	if total := attemptsTotal(w.metrics); total != 0 {
		t.Fatal("a discarded result must not be counted as an attempt")
	}
}

func TestMetricsStartAtZero(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry(), []string{"a", "b"})
	if n := testutil.CollectAndCount(m.attempts); n != 2*len(outcomes) {
		t.Fatalf("attempt series at start = %d, want every kind and outcome at zero", n)
	}
}

func TestWorkerObservesLagAndDuration(t *testing.T) {
	late := task(1, 0)
	late.RunAfter = t0.Add(-5 * time.Second)
	store := &fakeStore{queue: []Task{late}}
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { return nil }), nil, testCfg)

	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var m dto.Metric
	if err := w.metrics.lag.WithLabelValues(kindTest).(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	if h := m.GetHistogram(); h.GetSampleCount() != 1 || h.GetSampleSum() != 5 {
		t.Fatalf("lag histogram count=%d sum=%v, want one sample of 5s", h.GetSampleCount(), h.GetSampleSum())
	}
	var d dto.Metric
	if err := w.metrics.duration.WithLabelValues(kindTest).(prometheus.Metric).Write(&d); err != nil {
		t.Fatal(err)
	}
	if n := d.GetHistogram().GetSampleCount(); n != 1 {
		t.Fatalf("task_duration_seconds samples = %d, want 1", n)
	}
}

func TestRunOnceRunsBatchConcurrently(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, 0), task(2, 0), task(3, 0)}}
	started := make(chan struct{})
	release := make(chan struct{})
	w := newWorker(store, handlerFunc(func(context.Context, Task) error {
		started <- struct{}{}
		<-release
		return nil
	}), nil, testCfg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.RunOnce(context.Background())
	}()
	for range 3 {
		<-started // all three are running at the same time
	}
	close(release)
	<-done
	if len(store.settled) != 3 {
		t.Fatalf("settled %d tasks, want 3", len(store.settled))
	}
}

// A slow task blocks only its own slot: when another task finishes, Run
// claims the next one at once, without waiting for the slow one or a tick.
func TestRunClaimsIntoFreedSlots(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, 0), task(2, 0), task(3, 0)}, claims: make(chan int, 10)}
	slowStarted, slowRelease := make(chan struct{}), make(chan struct{})
	thirdRan := make(chan struct{})
	cfg := testCfg
	cfg.Concurrency = 2
	w := newWorker(store, handlerFunc(func(_ context.Context, t Task) error {
		switch t.ID {
		case 1:
			close(slowStarted)
			<-slowRelease
		case 3:
			close(thirdRan)
		}
		return nil
	}), nil, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()

	if limit := <-store.claims; limit != 2 {
		t.Fatalf("first claim asked for %d, want 2", limit)
	}
	<-slowStarted
	<-thirdRan // task 2 finished and freed its slot; task 1 still runs
	close(slowRelease)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if store.settledCount() != 3 {
		t.Fatalf("settled %d, want 3: Run must wait for running tasks", store.settledCount())
	}
}

// Shutdown must not cancel a running handler: the task finishes and is
// settled before Run returns.
func TestRunShutdownLetsRunningTaskFinish(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, 0)}}
	started := make(chan struct{})
	release := make(chan struct{})
	var handlerErr error
	w := newWorker(store, handlerFunc(func(ctx context.Context, _ Task) error {
		close(started)
		<-release
		handlerErr = ctx.Err()
		return nil
	}), nil, testCfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()

	<-started
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("handler ctx was cancelled: %v", handlerErr)
	}
	if len(store.settled) != 1 || store.settled[0].how != "done" {
		t.Fatalf("settled = %+v, want the task completed", store.settled)
	}
}

func TestRunWakesOnSignal(t *testing.T) {
	store := &fakeStore{claims: make(chan int, 4)}
	ran := make(chan struct{})
	wake := make(chan struct{}, 1)
	w := newWorker(store, handlerFunc(func(context.Context, Task) error {
		close(ran)
		return nil
	}), wake, testCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()

	<-store.claims // the first, empty claim has happened
	store.mu.Lock()
	store.queue = append(store.queue, task(1, 0))
	store.mu.Unlock()
	wake <- struct{}{} // IdleInterval is an hour: only the signal can start it

	<-ran
	cancel()
	<-done
}

// After StopClaiming the worker takes nothing new, even when woken, and Run
// returns once its context is cancelled. The worker is stopped while it is
// idle after a first claim, and woken with work waiting: cancelling before
// Run reaches a claim would pass even if StopClaiming did nothing.
func TestRunStopClaiming(t *testing.T) {
	store := &fakeStore{claims: make(chan int, 16)}
	wake := make(chan struct{}, 1)
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { return nil }), wake, testCfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	<-store.claims // the first, empty claim: Run is now idle in its select
	w.StopClaiming()
	w.StopClaiming() // idempotent
	store.mu.Lock()
	store.queue = append(store.queue, task(1, 0))
	store.mu.Unlock()
	// Three sends through a one-slot channel: the third completes only after
	// Run has received the second, so it has fully handled the first wake.
	for range 3 {
		wake <- struct{}{}
	}
	if n := len(store.claims); n != 0 {
		t.Fatalf("Run claimed %d more times after StopClaiming", n)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if store.settledCount() != 0 {
		t.Fatalf("settled %d tasks after StopClaiming, want none", store.settledCount())
	}
}

// A task claimed with its attempts spent had a run that never settled (its
// process died in the handler): it is buried without running again.
func TestWorkerBuriesATaskWhoseLastRunNeverSettled(t *testing.T) {
	store := &fakeStore{queue: []Task{task(1, testCfg.MaxAttempts)}}
	ran := false
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { ran = true; return nil }), nil, testCfg)
	if n, err := w.RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if ran || len(store.settled) != 1 || store.settled[0].how != "dead" || store.settled[0].cause != errNeverSettled.Error() {
		t.Fatalf("ran=%v settled=%+v, want dead without running", ran, store.settled)
	}
	if got := testutil.ToFloat64(w.metrics.attempts.WithLabelValues(kindTest, outcomeDead)); got != 1 {
		t.Fatalf("dead outcomes = %v, want 1", got)
	}
}

// Tasks claimed as Run is cancelled are given back at once, with their
// attempt, instead of waiting out their lease without running.
func TestRunGivesBackTasksClaimedAtCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	due := t0.Add(-time.Minute)
	store := &fakeStore{queue: []Task{{ID: 1, Kind: kindTest, RunAfter: due}, {ID: 2, Kind: kindTest, RunAfter: due}}, onClaim: cancel}
	ran := false
	w := newWorker(store, handlerFunc(func(context.Context, Task) error { ran = true; return nil }), nil, testCfg)
	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if ran || len(store.settled) != 2 {
		t.Fatalf("ran=%v settled=%+v, want both given back", ran, store.settled)
	}
	for _, s := range store.settled {
		if s.how != "postponed" || !s.runAfter.Equal(due) {
			t.Fatalf("settled %+v, want postponed to its own run_after", s)
		}
	}
}

func TestBackoffDelay(t *testing.T) {
	lowest := Backoff{Base: time.Second, Max: time.Minute, Rand: func(int64) int64 { return 0 }}
	highest := Backoff{Base: time.Second, Max: time.Minute, Rand: func(n int64) int64 { return n - 1 }}
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{attempt: 1, min: 500 * time.Millisecond, max: time.Second},
		{attempt: 2, min: time.Second, max: 2 * time.Second},
		{attempt: 4, min: 4 * time.Second, max: 8 * time.Second},
		{attempt: 7, min: 30 * time.Second, max: time.Minute},
		{attempt: 500, min: 30 * time.Second, max: time.Minute},
		{attempt: 0, min: 500 * time.Millisecond, max: time.Second},
	}
	for _, tt := range tests {
		if got := lowest.Delay(tt.attempt); got != tt.min {
			t.Errorf("attempt %d lowest = %s, want %s", tt.attempt, got, tt.min)
		}
		if got := highest.Delay(tt.attempt); got != tt.max {
			t.Errorf("attempt %d highest = %s, want %s", tt.attempt, got, tt.max)
		}
	}
}

func TestBackoffDefaultRandStaysInRange(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	for range 1000 {
		if d := b.Delay(3); d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("Delay(3) = %s, want within [2s, 4s]", d)
		}
	}
}
