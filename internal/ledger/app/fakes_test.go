package app

import (
	"cmp"
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// memStore is an in-memory TxRunner, AccountStore, HoldStore and Journal.
// Run executes one transaction at a time and restores a snapshot when fn
// fails or panics, so unit tests see real isolation and rollback.
type memStore struct {
	txMu sync.Mutex // serializes transactions

	mu       sync.Mutex
	accounts map[uuid.UUID]ledger.Account
	holds    map[uuid.UUID]ledger.Hold
	txns     map[string]ledger.Transaction // by idempotency key
	entries  []ledger.Entry
	nextID   int64
	busy     map[uuid.UUID]bool // holds that TryLockHold reports as locked
}

func newMemStore() *memStore {
	return &memStore{
		accounts: map[uuid.UUID]ledger.Account{},
		holds:    map[uuid.UUID]ledger.Hold{},
		txns:     map[string]ledger.Transaction{},
		busy:     map[uuid.UUID]bool{},
	}
}

type memState struct {
	accounts map[uuid.UUID]ledger.Account
	holds    map[uuid.UUID]ledger.Hold
	txns     map[string]ledger.Transaction
	entries  []ledger.Entry
	nextID   int64
}

func (s *memStore) snapshot() memState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return memState{maps.Clone(s.accounts), maps.Clone(s.holds), maps.Clone(s.txns), slices.Clone(s.entries), s.nextID}
}

func (s *memStore) restore(st memState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts, s.holds, s.txns, s.entries, s.nextID = st.accounts, st.holds, st.txns, st.entries, st.nextID
}

func (s *memStore) Run(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	snap := s.snapshot()
	defer func() {
		if p := recover(); p != nil {
			s.restore(snap)
			panic(p)
		}
	}()
	if err := fn(ctx); err != nil {
		s.restore(snap)
		return err
	}
	return nil
}

func (s *memStore) InsertAccount(_ context.Context, a ledger.Account) (ledger.Account, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.accounts {
		if existing.Kind == ledger.KindCustomer && existing.IdempotencyKey == a.IdempotencyKey {
			return existing, false, nil
		}
	}
	s.accounts[a.ID] = a
	return a, true, nil
}

func (s *memStore) Account(_ context.Context, id uuid.UUID) (ledger.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[id]
	if !ok {
		return ledger.Account{}, ledger.ErrAccountNotFound
	}
	return a, nil
}

func (s *memStore) LockAccounts(ctx context.Context, ids ...uuid.UUID) (map[uuid.UUID]ledger.Account, error) {
	out := make(map[uuid.UUID]ledger.Account, len(ids))
	for _, id := range ids {
		a, err := s.Account(ctx, id)
		if err != nil {
			return nil, err
		}
		out[id] = a
	}
	return out, nil
}

func (s *memStore) LockTreasury(_ context.Context, candidate ledger.Account) (ledger.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.accounts {
		if a.Kind == ledger.KindSystem && a.Currency == candidate.Currency {
			return a, nil
		}
	}
	s.accounts[candidate.ID] = candidate
	return candidate, nil
}

func (s *memStore) SaveBalances(_ context.Context, accounts ...ledger.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range accounts {
		cur := s.accounts[a.ID]
		cur.PostedMinor, cur.HeldMinor, cur.UpdatedAt = a.PostedMinor, a.HeldMinor, a.UpdatedAt
		s.accounts[a.ID] = cur
	}
	return nil
}

func (s *memStore) Accounts(_ context.Context, after uuid.UUID, limit int) ([]ledger.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := slices.SortedFunc(maps.Values(s.accounts), func(a, b ledger.Account) int {
		return cmp.Compare(a.ID.String(), b.ID.String())
	})
	var out []ledger.Account
	for _, a := range all {
		if a.ID.String() > after.String() && len(out) < limit {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *memStore) HoldByKey(_ context.Context, key string) (ledger.Hold, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.holds {
		if h.IdempotencyKey == key {
			return h, true, nil
		}
	}
	return ledger.Hold{}, false, nil
}

func (s *memStore) InsertHold(ctx context.Context, h ledger.Hold) (bool, error) {
	if _, ok, _ := s.HoldByKey(ctx, h.IdempotencyKey); ok {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holds[h.ID] = h
	return true, nil
}

func (s *memStore) LockHold(_ context.Context, id uuid.UUID) (ledger.Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.holds[id]
	if !ok {
		return ledger.Hold{}, ledger.ErrHoldNotFound
	}
	return h, nil
}

func (s *memStore) TryLockHold(ctx context.Context, id uuid.UUID) (ledger.Hold, bool, error) {
	s.mu.Lock()
	busy := s.busy[id]
	s.mu.Unlock()
	if busy {
		return ledger.Hold{}, false, nil
	}
	h, err := s.LockHold(ctx, id)
	return h, err == nil, err
}

func (s *memStore) SaveHold(_ context.Context, h ledger.Hold) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holds[h.ID] = h
	return nil
}

func (s *memStore) ExpiredHolds(_ context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uuid.UUID
	for _, h := range s.holds {
		if h.Status == ledger.HoldActive && h.Expired(now) && len(out) < limit {
			out = append(out, h.ID)
		}
	}
	return out, nil
}

func (s *memStore) TransactionByKey(_ context.Context, key string) (ledger.Transaction, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.txns[key]
	return t, ok, nil
}

func (s *memStore) InsertTransaction(_ context.Context, t ledger.Transaction) (ledger.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Entries = slices.Clone(t.Entries)
	for i := range t.Entries {
		s.nextID++
		t.Entries[i].ID = s.nextID
		s.entries = append(s.entries, t.Entries[i])
	}
	s.txns[t.IdempotencyKey] = t
	return t, nil
}

func (s *memStore) Entries(_ context.Context, accountID uuid.UUID, after int64, limit int) ([]ledger.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ledger.Entry
	for _, e := range s.entries {
		if e.AccountID == accountID && e.ID > after && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

// fakeMetrics counts business measurements.
type fakeMetrics struct {
	mu               sync.Mutex
	placed, captured int
	releasedByCause  map[string]int
}

func (m *fakeMetrics) HoldPlaced() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.placed++
}

func (m *fakeMetrics) HoldCaptured() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.captured++
}

func (m *fakeMetrics) HoldReleased(cause string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releasedByCause == nil {
		m.releasedByCause = map[string]int{}
	}
	m.releasedByCause[cause]++
}

// clock is a settable clock.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

type fixture struct {
	store   *memStore
	clock   *clock
	metrics *fakeMetrics
	svc     *Service
}

func newFixture() *fixture {
	f := &fixture{store: newMemStore(), clock: &clock{now: t0}, metrics: &fakeMetrics{}}
	f.svc = NewService(Deps{
		Tx:       f.store,
		Accounts: f.store,
		Holds:    f.store,
		Journal:  f.store,
		Clock:    f.clock,
		Metrics:  f.metrics,
		NewID:    uuid.NewV7,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, Config{DefaultHoldTTL: 15 * time.Minute, MaxHoldTTL: 24 * time.Hour})
	return f
}
