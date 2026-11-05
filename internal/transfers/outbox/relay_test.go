package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeStore struct {
	pending  []Record
	marked   []int64
	released []int64
	claims   chan struct{}
}

func (s *fakeStore) Claim(_ context.Context, _ time.Time, limit int, _ time.Duration) ([]Record, error) {
	if s.claims != nil {
		s.claims <- struct{}{}
	}
	n := min(limit, len(s.pending))
	out := s.pending[:n]
	s.pending = s.pending[n:]
	return out, nil
}

func (s *fakeStore) MarkPublished(_ context.Context, ids []int64, _ time.Time) error {
	s.marked = append(s.marked, ids...)
	return nil
}

func (s *fakeStore) Release(_ context.Context, ids []int64) error {
	s.released = append(s.released, ids...)
	return nil
}

type fakePublisher struct {
	failOn int64
	seen   []int64
	clock  *stepClock // advanced by every delivery when set
}

func (p *fakePublisher) Publish(ctx context.Context, r Record) error {
	if p.clock != nil {
		p.clock.advance(20 * time.Second)
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("a delivery must have a deadline")
	}
	if r.ID == p.failOn {
		return errors.New("broker unavailable")
	}
	p.seen = append(p.seen, r.ID)
	return nil
}

type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func records(ids ...int64) []Record {
	out := make([]Record, len(ids))
	for i, id := range ids {
		out[i] = Record{ID: id, Type: "transfer.created"}
	}
	return out
}

var testConfig = Config{Interval: time.Hour, Batch: 10, Lease: time.Minute, PublishTimeout: 5 * time.Second}

func newRelay(store Store, pub Publisher, clock Clock, wake <-chan struct{}) *Relay {
	return NewRelay(store, pub, clock, wake, NewMetrics(prometheus.NewRegistry()), testConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newClock() *stepClock { return &stepClock{now: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)} }

func TestRelayPublishesAndMarksInOrder(t *testing.T) {
	store := &fakeStore{pending: records(1, 2, 3)}
	pub := &fakePublisher{}

	n, err := newRelay(store, pub, newClock(), nil).RunOnce(context.Background())

	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if !slices.Equal(pub.seen, []int64{1, 2, 3}) || !slices.Equal(store.marked, []int64{1, 2, 3}) || len(store.released) != 0 {
		t.Fatalf("published %v, marked %v, released %v", pub.seen, store.marked, store.released)
	}
}

func TestRelayStopsAtFirstFailureAndReleasesTheRest(t *testing.T) {
	store := &fakeStore{pending: records(1, 2, 3)}
	pub := &fakePublisher{failOn: 2}

	relay := newRelay(store, pub, newClock(), nil)
	n, err := relay.RunOnce(context.Background())

	if err == nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v; want 1 and an error", n, err)
	}
	if p, f := testutil.ToFloat64(relay.metrics.published), testutil.ToFloat64(relay.metrics.failed); p != 1 || f != 1 {
		t.Fatalf("outbox_published_total = %v, outbox_failed_total = %v; want 1 and 1", p, f)
	}
	if !slices.Equal(store.marked, []int64{1}) || !slices.Equal(store.released, []int64{2, 3}) {
		t.Fatalf("marked %v, released %v", store.marked, store.released)
	}
}

// A slow broker must not keep publishing past the lease: another relay
// could then claim the same events and publish them a second time.
func TestRelayStopsBeforeTheLeaseRunsOut(t *testing.T) {
	clock := newClock()
	store := &fakeStore{pending: records(1, 2, 3, 4, 5)}
	pub := &fakePublisher{clock: clock} // every delivery takes 20 s

	n, err := newRelay(store, pub, clock, nil).RunOnce(context.Background())

	// Lease 60 s, timeout 5 s: deliveries start at 0, 20 and 40 s; at 60 s
	// there is no room for another one.
	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v; want 3", n, err)
	}
	if !slices.Equal(store.released, []int64{4, 5}) {
		t.Fatalf("released %v, want the events the lease had no room for", store.released)
	}
}

// A wake-up starts a batch without waiting for the tick.
func TestRelayWakesOnSignal(t *testing.T) {
	store := &fakeStore{claims: make(chan struct{})}
	wake := make(chan struct{}, 1)
	relay := newRelay(store, &fakePublisher{}, newClock(), wake)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- relay.Run(ctx) }()

	<-store.claims // the first claim, at start
	wake <- struct{}{}
	<-store.claims // the woken claim; the interval is an hour
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}
