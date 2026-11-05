package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// errCrash is the panic value of a simulated process crash.
var errCrash = errors.New("simulated crash")

// memStore is an in-memory TxRunner, TransferStore, Enqueuer, EventRecorder
// and task queue. Run executes one transaction at a time and restores a
// snapshot when fn fails or panics, so tests see real rollback.
//
// crashBefore and crashAfter number transactions from 1: the one numbered
// crashBefore is rolled back and panics, like a process killed just before
// COMMIT; the one numbered crashAfter commits and then panics, like a
// process killed just after it.
type memStore struct {
	txMu sync.Mutex

	mu        sync.Mutex
	transfers map[uuid.UUID]transfers.Transfer
	tasks     []*memTask
	events    []transfers.Event
	nextTask  int64

	runs        int
	crashBefore int
	crashAfter  int
}

type memTask struct {
	task  taskqueue.Task
	state string // ready, done, dead
}

func newMemStore() *memStore {
	return &memStore{transfers: map[uuid.UUID]transfers.Transfer{}}
}

type memState struct {
	transfers map[uuid.UUID]transfers.Transfer
	tasks     []memTask
	events    []transfers.Event
	nextTask  int64
}

func (s *memStore) snapshot() memState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := memState{transfers: map[uuid.UUID]transfers.Transfer{}, events: slices.Clone(s.events), nextTask: s.nextTask}
	for id, t := range s.transfers {
		t.Steps = slices.Clone(t.Steps)
		st.transfers[id] = t
	}
	for _, t := range s.tasks {
		st.tasks = append(st.tasks, *t)
	}
	return st
}

func (s *memStore) restore(st memState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transfers, s.events, s.nextTask = st.transfers, st.events, st.nextTask
	s.tasks = nil
	for _, t := range st.tasks {
		s.tasks = append(s.tasks, &t)
	}
}

func (s *memStore) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	snap := s.snapshot()
	s.mu.Lock()
	s.runs++
	n := s.runs
	s.mu.Unlock()
	committed := false
	defer func() {
		if p := recover(); p != nil {
			if !committed {
				s.restore(snap)
			}
			panic(p)
		}
	}()
	if err := fn(ctx); err != nil {
		s.restore(snap)
		return err
	}
	if n == s.crashBefore {
		panic(errCrash) // the deferred restore undoes the transaction
	}
	committed = true
	if n == s.crashAfter {
		panic(errCrash)
	}
	return nil
}

func (s *memStore) Insert(_ context.Context, t transfers.Transfer) (transfers.Transfer, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.transfers {
		if existing.IdempotencyKey == t.IdempotencyKey {
			return existing, false, nil
		}
	}
	s.transfers[t.ID] = t
	return t, true, nil
}

func (s *memStore) Transfer(_ context.Context, id uuid.UUID) (transfers.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.transfers[id]
	if !ok {
		return transfers.Transfer{}, transfers.ErrNotFound
	}
	t.Steps = slices.Clone(t.Steps)
	return t, nil
}

func (s *memStore) Lock(ctx context.Context, id uuid.UUID) (transfers.Transfer, error) {
	return s.Transfer(ctx, id)
}

func (s *memStore) Update(_ context.Context, t transfers.Transfer, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.transfers[t.ID]
	if !ok {
		return transfers.ErrNotFound
	}
	if cur.Version != expectedVersion {
		return transfers.ErrVersionConflict
	}
	t.Steps = slices.Clone(t.Steps)
	s.transfers[t.ID] = t
	return nil
}

func (s *memStore) Record(_ context.Context, e transfers.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *memStore) Enqueue(_ context.Context, spec taskqueue.Spec) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.task.Kind == spec.Kind && t.task.AggregateID == spec.AggregateID && t.task.DedupKey == spec.DedupKey {
			return false, nil
		}
	}
	s.nextTask++
	s.tasks = append(s.tasks, &memTask{state: "ready", task: taskqueue.Task{
		ID: s.nextTask, Kind: spec.Kind, AggregateID: spec.AggregateID, DedupKey: spec.DedupKey,
		Payload: spec.Payload, RunAfter: spec.RunAfter,
	}})
	return true, nil
}

func (s *memStore) Claim(_ context.Context, now time.Time, limit int, lease time.Duration) ([]taskqueue.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []taskqueue.Task
	for _, t := range s.tasks {
		if len(out) == limit {
			break
		}
		if t.state == "ready" && !t.task.RunAfter.After(now) && !t.task.LeaseUntil.After(now) {
			t.task.LeaseUntil = now.Add(lease)
			out = append(out, t.task)
		}
	}
	return out, nil
}

func (s *memStore) settle(t taskqueue.Task, change func(*memTask)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, mt := range s.tasks {
		if mt.task.ID == t.ID {
			if !mt.task.LeaseUntil.Equal(t.LeaseUntil) || mt.state != "ready" {
				return taskqueue.ErrLeaseLost
			}
			change(mt)
			mt.task.LeaseUntil = time.Time{}
			return nil
		}
	}
	return taskqueue.ErrLeaseLost
}

func (s *memStore) Complete(_ context.Context, t taskqueue.Task) error {
	return s.settle(t, func(mt *memTask) { mt.state = "done" })
}

func (s *memStore) Retry(_ context.Context, t taskqueue.Task, runAfter time.Time, _ string) error {
	return s.settle(t, func(mt *memTask) { mt.task.Attempts++; mt.task.RunAfter = runAfter })
}

func (s *memStore) Postpone(_ context.Context, t taskqueue.Task, runAfter time.Time) error {
	return s.settle(t, func(mt *memTask) { mt.task.RunAfter = runAfter })
}

func (s *memStore) Bury(_ context.Context, t taskqueue.Task, _ string) error {
	return s.settle(t, func(mt *memTask) { mt.task.Attempts++; mt.state = "dead" })
}

// nextDue returns the earliest run_after of a ready task.
func (s *memStore) nextDue() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due time.Time
	found := false
	for _, t := range s.tasks {
		if t.state == "ready" && (!found || t.task.RunAfter.Before(due)) {
			due, found = t.task.RunAfter, true
		}
	}
	return due, found
}

func (s *memStore) taskStates() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for _, t := range s.tasks {
		out[t.task.Kind] = t.state
	}
	return out
}

func (s *memStore) eventTypes(id uuid.UUID) []transfers.EventType {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []transfers.EventType
	for _, e := range s.events {
		if e.Transfer.ID == id {
			out = append(out, e.Type)
		}
	}
	return out
}

// fakeLedger keeps balances and holds in memory with the ledger's
// idempotency rules: a key is executed once and replayed afterwards.
//
// fail runs before an operation and may refuse it (nothing executed);
// after runs once the operation has been executed: it may panic, like a
// process that dies before it reads the answer, or return an error, like
// an answer lost on the way back.
type fakeLedger struct {
	mu       sync.Mutex
	clock    Clock
	accounts map[uuid.UUID]*Account
	holds    map[uuid.UUID]*fakeHold
	byKey    map[string]uuid.UUID // hold key -> hold
	captures map[string]uuid.UUID // capture key -> transaction
	released map[string]bool      // release keys executed

	placed, captured, releasedCount int

	fail  func(op string) error
	after func(op string) error
}

type fakeHold struct {
	id, src, dst uuid.UUID
	amount       int64
	status       string
	expiresAt    time.Time
}

func newFakeLedger(clock Clock) *fakeLedger {
	return &fakeLedger{
		clock:    clock,
		accounts: map[uuid.UUID]*Account{},
		holds:    map[uuid.UUID]*fakeHold{},
		byKey:    map[string]uuid.UUID{},
		captures: map[string]uuid.UUID{},
		released: map[string]bool{},
	}
}

func (l *fakeLedger) open(name string, balance int64) uuid.UUID {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := uuid.New()
	l.accounts[id] = &Account{ID: id, Name: name, Currency: "EUR", PostedMinor: balance, AvailableMinor: balance}
	return id
}

func (l *fakeLedger) account(id uuid.UUID) Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *l.accounts[id]
}

func (l *fakeLedger) hooks(op string) error {
	l.mu.Lock()
	fail := l.fail
	l.mu.Unlock()
	if fail != nil {
		return fail(op)
	}
	return nil
}

func (l *fakeLedger) done(op string) error {
	l.mu.Lock()
	after := l.after
	l.mu.Unlock()
	if after != nil {
		return after(op)
	}
	return nil
}

func (l *fakeLedger) PlaceHold(_ context.Context, req HoldRequest) (Hold, error) {
	if err := l.hooks("place"); err != nil {
		return Hold{}, err
	}
	l.mu.Lock()
	if id, ok := l.byKey[req.Key]; ok {
		l.mu.Unlock()
		if err := l.done("place"); err != nil {
			return Hold{}, err
		}
		return Hold{ID: id, Replayed: true}, nil
	}
	src, ok1 := l.accounts[req.SourceID]
	dst, ok2 := l.accounts[req.DestinationID]
	switch {
	case !ok1 || !ok2:
		l.mu.Unlock()
		return Hold{}, ErrAccountNotFound
	case src.Currency != req.Currency || dst.Currency != req.Currency:
		l.mu.Unlock()
		return Hold{}, ErrCurrencyMismatch
	case src.AvailableMinor < req.AmountMinor:
		l.mu.Unlock()
		return Hold{}, ErrInsufficientFunds
	}
	h := &fakeHold{id: uuid.New(), src: req.SourceID, dst: req.DestinationID, amount: req.AmountMinor,
		status: "active", expiresAt: l.clock.Now().Add(req.TTL)}
	l.holds[h.id], l.byKey[req.Key] = h, h.id
	src.HeldMinor += h.amount
	src.AvailableMinor -= h.amount
	l.placed++
	l.mu.Unlock()
	if err := l.done("place"); err != nil {
		return Hold{}, err
	}
	return Hold{ID: h.id}, nil
}

func (l *fakeLedger) CaptureHold(_ context.Context, holdID uuid.UUID, key string) (Capture, error) {
	if err := l.hooks("capture"); err != nil {
		return Capture{}, err
	}
	l.mu.Lock()
	if tx, ok := l.captures[key]; ok {
		l.mu.Unlock()
		if err := l.done("capture"); err != nil {
			return Capture{}, err
		}
		return Capture{TransactionID: tx, Replayed: true}, nil
	}
	h, ok := l.holds[holdID]
	switch {
	case !ok:
		l.mu.Unlock()
		return Capture{}, fmt.Errorf("%w: unknown hold", ErrLedgerContract)
	case h.status == "captured":
		l.mu.Unlock()
		return Capture{}, ErrHoldCaptured
	case h.status == "released" || !l.clock.Now().Before(h.expiresAt):
		l.mu.Unlock()
		return Capture{}, ErrHoldExpired
	}
	src, dst := l.accounts[h.src], l.accounts[h.dst]
	src.HeldMinor -= h.amount
	src.PostedMinor -= h.amount
	dst.PostedMinor += h.amount
	dst.AvailableMinor += h.amount
	h.status = "captured"
	tx := uuid.New()
	l.captures[key] = tx
	l.captured++
	l.mu.Unlock()
	if err := l.done("capture"); err != nil {
		return Capture{}, err
	}
	return Capture{TransactionID: tx}, nil
}

func (l *fakeLedger) ReleaseHold(_ context.Context, holdID uuid.UUID, key, _ string) (Release, error) {
	if err := l.hooks("release"); err != nil {
		return Release{}, err
	}
	l.mu.Lock()
	h, ok := l.holds[holdID]
	switch {
	case !ok:
		l.mu.Unlock()
		return Release{}, fmt.Errorf("%w: unknown hold", ErrLedgerContract)
	case h.status == "captured":
		l.mu.Unlock()
		return Release{}, ErrHoldCaptured
	case h.status == "released":
		l.mu.Unlock()
		if err := l.done("release"); err != nil {
			return Release{}, err
		}
		return Release{Replayed: true}, nil
	}
	src := l.accounts[h.src]
	src.HeldMinor -= h.amount
	src.AvailableMinor += h.amount
	h.status = "released"
	l.released[key] = true
	l.releasedCount++
	l.mu.Unlock()
	if err := l.done("release"); err != nil {
		return Release{}, err
	}
	return Release{}, nil
}

func (l *fakeLedger) Accounts(context.Context) ([]Account, error) {
	if err := l.hooks("accounts"); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Account, 0, len(l.accounts))
	for _, a := range l.accounts {
		out = append(out, *a)
	}
	slices.SortFunc(out, func(a, b Account) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

func (l *fakeLedger) counts() (placed, captured, released int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.placed, l.captured, l.releasedCount
}

// fakeScreener answers by amount like the fake provider (..66 declined,
// ..77 reviewed then approved) and deduplicates by key. fail and after work
// like the ledger's.
type fakeScreener struct {
	mu          sync.Mutex
	clock       Clock
	reviewDelay time.Duration
	byKey       map[string]*fakeScreening
	executions  int
	keys        []string

	fail  func() error
	after func() error
}

type fakeScreening struct {
	id          string
	final       Decision
	reviewUntil time.Time
}

func newFakeScreener(clock Clock) *fakeScreener {
	return &fakeScreener{clock: clock, reviewDelay: 10 * time.Second, byKey: map[string]*fakeScreening{}}
}

func (s *fakeScreener) Screen(_ context.Context, req ScreeningRequest) (Screening, error) {
	s.mu.Lock()
	fail, after := s.fail, s.after
	s.keys = append(s.keys, req.Key)
	s.mu.Unlock()
	if fail != nil {
		if err := fail(); err != nil {
			return Screening{}, err
		}
	}
	s.mu.Lock()
	sc, ok := s.byKey[req.Key]
	if !ok {
		sc = &fakeScreening{id: "scr-" + req.TransferID.String()[:8], final: DecisionApprove}
		switch req.AmountMinor % 100 {
		case 66:
			sc.final = DecisionDecline
		case 77:
			sc.reviewUntil = s.clock.Now().Add(s.reviewDelay)
		}
		s.byKey[req.Key] = sc
		s.executions++
	}
	res := Screening{ID: sc.id, Decision: sc.final}
	if s.clock.Now().Before(sc.reviewUntil) {
		res = Screening{ID: sc.id, Decision: DecisionReview, RetryAfter: sc.reviewUntil.Sub(s.clock.Now())}
	}
	s.mu.Unlock()
	if after != nil {
		if err := after(); err != nil {
			return Screening{}, err
		}
	}
	return res, nil
}

func (s *fakeScreener) stats() (executions int, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executions, slices.Clone(s.keys)
}

// fakeWatcher hands out channels the test can fire. watched, when set,
// receives the id of every Watch call.
type fakeWatcher struct {
	mu      sync.Mutex
	err     error
	waiters map[uuid.UUID]chan int64
	watched chan uuid.UUID
}

func (w *fakeWatcher) Watch(id uuid.UUID, _ int64) (<-chan int64, func(), error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return nil, nil, w.err
	}
	if w.waiters == nil {
		w.waiters = map[uuid.UUID]chan int64{}
	}
	ch := make(chan int64, 1)
	w.waiters[id] = ch
	if w.watched != nil {
		w.watched <- id
	}
	return ch, func() {}, nil
}

func (w *fakeWatcher) fire(id uuid.UUID, v int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waiters[id] <- v
}

// fakeMetrics records business measurements.
type fakeMetrics struct {
	mu       sync.Mutex
	created  int
	statuses []string
	steps    map[string]int
}

func (m *fakeMetrics) TransferCreated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created++
}

func (m *fakeMetrics) StatusChanged(from, to transfers.Status, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses = append(m.statuses, string(from)+"->"+string(to))
}

func (m *fakeMetrics) StepFinished(step transfers.Step, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.steps == nil {
		m.steps = map[string]int{}
	}
	m.steps[string(step)+":"+outcome]++
}

// clock is a settable clock shared by the service, the fakes and the worker.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.now) {
		c.now = t
	}
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

func testConfig() Config {
	return Config{
		HoldTTL:             15 * time.Minute,
		ScreenMaxFailures:   4,
		ScreenReviewTimeout: 2 * time.Minute,
		ScreenPollMin:       time.Second,
		ScreenPollMax:       30 * time.Second,
		WaitTimeout:         time.Second,
		WaitReadTimeout:     time.Second,
	}
}

// fixture is a Service on fakes, with the real task queue worker.
type fixture struct {
	store    *memStore
	ledger   *fakeLedger
	screener *fakeScreener
	watcher  *fakeWatcher
	metrics  *fakeMetrics
	clock    *clock
	svc      *Service
	worker   *taskqueue.Worker
	src, dst uuid.UUID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{store: newMemStore(), clock: &clock{now: t0}, watcher: &fakeWatcher{}, metrics: &fakeMetrics{}}
	f.ledger = newFakeLedger(f.clock)
	f.screener = newFakeScreener(f.clock)
	f.src = f.ledger.open("Clover", 10_000)
	f.dst = f.ledger.open("Pip", 0)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.svc = NewService(Deps{
		Tx: f.store, Transfers: f.store, Tasks: f.store, Events: f.store,
		Ledger: f.ledger, Screener: f.screener, Watcher: f.watcher,
		Clock: f.clock, Metrics: f.metrics, NewID: uuid.NewV7, Logger: log,
	}, testConfig())
	f.worker = taskqueue.NewWorker(f.store, f.svc, f.clock, nil,
		taskqueue.NewMetrics(prometheus.NewRegistry(), TaskKinds), log, taskqueue.Config{
			Concurrency:  4,
			IdleInterval: time.Hour,
			Lease:        time.Minute,
			TaskTimeout:  5 * time.Second,
			MaxAttempts:  10,
			Backoff:      taskqueue.Backoff{Base: 100 * time.Millisecond, Max: time.Second, Rand: func(int64) int64 { return 0 }},
		})
	return f
}

// create accepts a transfer of amount, retrying like a client would after a
// crash: with the same Idempotency-Key until it gets an answer.
func (f *fixture) create(t *testing.T, amount int64) uuid.UUID {
	t.Helper()
	for range 5 {
		res, err := func() (res CreateTransferResult, err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("crashed: %v", p)
				}
			}()
			return f.svc.CreateTransfer(context.Background(), CreateTransferRequest{
				IdempotencyKey: "client-key", SourceAccountID: f.src, DestinationAccountID: f.dst,
				AmountMinor: amount, Currency: "EUR",
			})
		}()
		if err == nil {
			return res.Transfer.ID
		}
	}
	t.Fatal("create never succeeded")
	return uuid.Nil
}

// drain runs the worker until no task is ready, moving the clock to the
// next due task whenever nothing is due now.
func (f *fixture) drain(t *testing.T) {
	t.Helper()
	for range 1000 {
		n, err := f.worker.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			continue
		}
		due, ok := f.store.nextDue()
		if !ok {
			return
		}
		f.clock.Set(due)
	}
	t.Fatal("the saga did not settle in 1000 rounds")
}

func (f *fixture) transfer(t *testing.T, id uuid.UUID) transfers.Transfer {
	t.Helper()
	tr, err := f.store.Transfer(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}
