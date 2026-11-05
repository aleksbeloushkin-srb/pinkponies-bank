package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
)

// fakeClient hands out the scripted polls, then closes drained and blocks
// until ctx ends. The consumer polls again only after it has handled and
// committed the previous poll, so drained means "everything is settled".
type fakeClient struct {
	mu        sync.Mutex
	polls     []kgo.Fetches
	committed []*kgo.Record
	allows    int
	drained   chan struct{}
	once      sync.Once
}

func (c *fakeClient) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	c.mu.Lock()
	if len(c.polls) > 0 {
		f := c.polls[0]
		c.polls = c.polls[1:]
		c.mu.Unlock()
		return f
	}
	c.mu.Unlock()
	c.once.Do(func() { close(c.drained) })
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (c *fakeClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.committed = append(c.committed, rs...)
	return nil
}

func (c *fakeClient) AllowRebalance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allows++
}

func (c *fakeClient) offsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int64
	for _, r := range c.committed {
		out = append(out, int64(r.Partition)*100+r.Offset)
	}
	return out
}

type fakeDLQ struct {
	mu       sync.Mutex
	failures int
	got      []*kgo.Record
	attempts []int
}

func (d *fakeDLQ) Publish(_ context.Context, r *kgo.Record, _ error, attempts int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failures > 0 {
		d.failures--
		return errors.New("dlq broker unavailable")
	}
	d.got = append(d.got, r)
	d.attempts = append(d.attempts, attempts)
	return nil
}

type handlerFunc func(m app.Message) (string, error)

func (f handlerFunc) HandleEvent(_ context.Context, m app.Message) (string, error) { return f(m) }

func poll(partition int32, offsets ...int64) kgo.Fetches {
	var recs []*kgo.Record
	for _, o := range offsets {
		recs = append(recs, &kgo.Record{Topic: "transfers.events", Partition: partition, Offset: o, Value: fmt.Appendf(nil, "%d/%d", partition, o)})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "transfers.events", Partitions: []kgo.FetchPartition{{Partition: partition, Records: recs}}}}}}
}

type harness struct {
	client *fakeClient
	dlq    *fakeDLQ
	c      *Consumer
	sleeps []time.Duration
	cancel context.CancelFunc
	ctx    context.Context
}

func newHarness(polls []kgo.Fetches, h Handler) *harness {
	hs := &harness{client: &fakeClient{polls: polls, drained: make(chan struct{})}, dlq: &fakeDLQ{}}
	hs.ctx, hs.cancel = context.WithCancel(context.Background())
	hs.c = New(hs.client, hs.dlq, h, NewMetrics(prometheus.NewRegistry()), Config{
		MaxPollRecords: 100, MaxAttempts: 3,
		Backoff:       taskqueue.Backoff{Base: 100 * time.Millisecond, Max: time.Second, Rand: func(int64) int64 { return 0 }},
		HandleTimeout: time.Second, CommitTimeout: time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	hs.c.sleep = func(ctx context.Context, d time.Duration) error {
		hs.sleeps = append(hs.sleeps, d)
		return ctx.Err()
	}
	return hs
}

// run runs the consumer until every scripted poll is handled, then stops
// it.
func (h *harness) run(t *testing.T) {
	t.Helper()
	done := make(chan error)
	go func() { done <- h.c.Run(h.ctx) }()
	select {
	case <-h.client.drained:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	}
	h.cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestHandledRecordsAreCommittedInOrderPerPartition(t *testing.T) {
	var seen []string
	h := newHarness([]kgo.Fetches{poll(0, 1, 2, 3), poll(1, 7, 8)}, handlerFunc(func(m app.Message) (string, error) {
		seen = append(seen, string(m.Value))
		return app.OutcomeStored, nil
	}))
	h.run(t)

	if !slices.Equal(seen, []string{"0/1", "0/2", "0/3", "1/7", "1/8"}) {
		t.Fatalf("handled %v", seen)
	}
	if got := h.client.offsets(); !slices.Equal(got, []int64{1, 2, 3, 107, 108}) {
		t.Fatalf("committed %v", got)
	}
	if got := testutil.ToFloat64(h.c.metrics.records.WithLabelValues(app.OutcomeStored)); got != 5 {
		t.Fatalf("stored = %v", got)
	}
}

// A duplicate is a handled record: committed, never retried.
func TestDuplicateIsCommitted(t *testing.T) {
	h := newHarness([]kgo.Fetches{poll(0, 5)}, handlerFunc(func(app.Message) (string, error) { return app.OutcomeDuplicate, nil }))
	h.run(t)
	if len(h.sleeps) != 0 || testutil.ToFloat64(h.c.metrics.records.WithLabelValues(app.OutcomeDuplicate)) != 1 {
		t.Fatalf("sleeps %v", h.sleeps)
	}
}

// A poison message is tried MaxAttempts times with growing backoff, then
// written to the dead-letter topic and committed; the next record goes on.
func TestPoisonGoesToTheDLQAfterRetries(t *testing.T) {
	h := newHarness([]kgo.Fetches{poll(0, 1, 2)}, handlerFunc(func(m app.Message) (string, error) {
		if string(m.Value) == "0/1" {
			return "", fmt.Errorf("%w: not JSON", notifications.ErrInvalidEvent)
		}
		return app.OutcomeStored, nil
	}))
	h.run(t)

	if len(h.dlq.got) != 1 || h.dlq.got[0].Offset != 1 || h.dlq.attempts[0] != 3 {
		t.Fatalf("dead-lettered %v with attempts %v", h.dlq.got, h.dlq.attempts)
	}
	if !slices.Equal(h.sleeps, []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}) {
		t.Fatalf("backoff = %v, want two growing pauses", h.sleeps)
	}
	if got := h.client.offsets(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("committed %v: the dead-lettered record is committed too", got)
	}
	if testutil.ToFloat64(h.c.metrics.records.WithLabelValues(outcomeDeadLettered)) != 1 ||
		testutil.ToFloat64(h.c.metrics.retries.WithLabelValues("poison")) != 2 {
		t.Fatal("metrics")
	}
}

// A transient failure (the database) is retried as long as it takes and
// never sent to the dead-letter topic.
func TestTransientFailureIsRetriedNotDeadLettered(t *testing.T) {
	fails := 5
	h := newHarness([]kgo.Fetches{poll(0, 1)}, handlerFunc(func(app.Message) (string, error) {
		if fails > 0 {
			fails--
			return "", errors.New("database unavailable")
		}
		return app.OutcomeStored, nil
	}))
	h.run(t)
	if len(h.dlq.got) != 0 || len(h.sleeps) != 5 {
		t.Fatalf("dlq %d, sleeps %v", len(h.dlq.got), h.sleeps)
	}
}

// When the consumer stops while a record keeps failing, that record and
// the ones after it are not committed; those before it are.
func TestOffsetIsNotCommittedOnFailure(t *testing.T) {
	h := newHarness([]kgo.Fetches{poll(0, 1, 2, 3)}, nil)
	h.c.handler = handlerFunc(func(m app.Message) (string, error) {
		if string(m.Value) == "0/2" {
			h.cancel() // the process is told to stop while this record fails
			return "", errors.New("database unavailable")
		}
		return app.OutcomeStored, nil
	})
	done := make(chan error)
	go func() { done <- h.c.Run(h.ctx) }()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if got := h.client.offsets(); !slices.Equal(got, []int64{1}) {
		t.Fatalf("committed %v, want only offset 1", got)
	}
}

// A failed write to the dead-letter topic is retried: the record must be
// somewhere before its offset is committed.
func TestDeadLetterWriteIsRetried(t *testing.T) {
	h := newHarness([]kgo.Fetches{poll(0, 1)}, handlerFunc(func(app.Message) (string, error) {
		return "", notifications.ErrInvalidEvent
	}))
	h.dlq.failures = 2
	h.run(t)
	if len(h.dlq.got) != 1 {
		t.Fatalf("dead-lettered %d", len(h.dlq.got))
	}
}

type fakeProducer struct{ got []*kgo.Record }

func (p *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.got = append(p.got, rs...)
	return kgo.ProduceResults{{Record: rs[0]}}
}

func TestDeadLetterTopicHeaders(t *testing.T) {
	p := &fakeProducer{}
	r := &kgo.Record{Topic: "transfers.events", Partition: 2, Offset: 41, Key: []byte("k"), Value: []byte("v"),
		Headers: []kgo.RecordHeader{{Key: "event-id", Value: []byte("e1")}}}
	if err := NewDeadLetterTopic(p, "transfers.events.dlq").Publish(context.Background(), r, errors.New("not JSON"), 5); err != nil {
		t.Fatal(err)
	}
	got := p.got[0]
	headers := map[string]string{}
	for _, h := range got.Headers {
		headers[h.Key] = string(h.Value)
	}
	if got.Topic != "transfers.events.dlq" || string(got.Key) != "k" || string(got.Value) != "v" || headers["event-id"] != "e1" ||
		headers[HeaderDLQError] != "not JSON" || headers[HeaderDLQAttempts] != "5" ||
		headers[HeaderDLQOriginalTopic] != "transfers.events" || headers[HeaderDLQOriginalPartition] != "2" || headers[HeaderDLQOriginalOffset] != "41" {
		t.Fatalf("record %+v headers %v", got, headers)
	}
}
