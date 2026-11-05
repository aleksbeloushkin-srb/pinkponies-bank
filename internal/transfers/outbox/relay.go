// Package outbox publishes the integration events recorded in the
// transfers service's outbox table.
//
// The Relay claims a batch of unpublished events with a lease (one UPDATE
// that commits at once), publishes them one by one in id order with no
// transaction open, and then marks the published ones. Delivery is at
// least once: a crash between publishing and marking publishes the event
// again after the lease expires, so consumers deduplicate by event id.
//
// Events of one transfer keep their order: they are published in id order,
// on the same Kafka partition (the key is the transfer id), and the relay
// stops at the first failure so that a later event never overtakes an
// earlier one.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// Record is a stored event waiting to be published.
type Record struct {
	ID          int64
	EventID     uuid.UUID
	AggregateID uuid.UUID
	Type        string
	Payload     []byte
	CreatedAt   time.Time
}

// Store claims and marks outbox events.
type Store interface {
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]Record, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
	Release(ctx context.Context, ids []int64) error
}

// Publisher delivers one event to the outside world.
type Publisher interface {
	Publish(ctx context.Context, r Record) error
}

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// Metrics are the relay's Prometheus metrics.
type Metrics struct {
	published prometheus.Counter
	failed    prometheus.Counter
	duration  prometheus.Histogram
}

// NewMetrics creates the relay metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Outbox events published and marked.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_failed_total",
			Help: "Failed deliveries; the event is released and published again later.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_publish_duration_seconds",
			Help:    "Duration of one delivery to the broker, until it acknowledged.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
	}
	reg.MustRegister(m.published, m.failed, m.duration)
	return m
}

// settleTimeout bounds the statements that record a batch's outcome.
const settleTimeout = 5 * time.Second

// Config tunes a Relay.
type Config struct {
	// Interval is how often an idle relay looks for new events.
	Interval time.Duration
	// Batch is the most events claimed at once.
	Batch int
	// Lease is how long claimed events stay invisible to other relays.
	Lease time.Duration
	// PublishTimeout bounds one delivery. A delivery starts only if it can
	// finish before the lease expires.
	PublishTimeout time.Duration
}

// Relay moves events from the outbox to a Publisher.
type Relay struct {
	store   Store
	pub     Publisher
	clock   Clock
	wake    <-chan struct{}
	metrics *Metrics
	cfg     Config
	log     *slog.Logger
}

// NewRelay returns a Relay. wake may be nil; a value on it starts a batch
// without waiting for the next tick.
func NewRelay(store Store, pub Publisher, clock Clock, wake <-chan struct{}, m *Metrics, cfg Config, log *slog.Logger) *Relay {
	return &Relay{store: store, pub: pub, clock: clock, wake: wake, metrics: m, cfg: cfg, log: log}
}

// Run publishes until ctx is cancelled. A full batch is followed by another
// one at once; a failed batch is logged and retried on the next tick.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.log.WarnContext(ctx, "outbox relay batch failed", "err", err)
		}
		if err == nil && n == r.cfg.Batch {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

// RunOnce claims one batch, publishes it in id order and records the
// outcome. It returns the number of events published.
//
// Publishing stops at the first failure so that a later event of the same
// transfer never overtakes an earlier one. Events published before the
// failure are marked; the rest are released for the next batch.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	claimedAt := r.clock.Now()
	records, err := r.store.Claim(ctx, claimedAt, r.cfg.Batch, r.cfg.Lease)
	if err != nil {
		return 0, err
	}
	leaseEnd := claimedAt.Add(r.cfg.Lease)

	var (
		published []int64
		pubErr    error
	)
	for _, rec := range records {
		// Starting a delivery that could outlive the lease would let another
		// relay claim and publish the same event at the same time.
		if r.clock.Now().Add(r.cfg.PublishTimeout).After(leaseEnd) {
			break
		}
		if err := r.publish(ctx, rec); err != nil {
			pubErr = fmt.Errorf("publish outbox event %d: %w", rec.ID, err)
			break
		}
		published = append(published, rec.ID)
	}

	// The deliveries have happened: record them even if ctx was cancelled
	// meanwhile, otherwise they would be published again.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if len(published) > 0 {
		if err := r.store.MarkPublished(sctx, published, r.clock.Now()); err != nil {
			return 0, fmt.Errorf("mark %d published events: %w", len(published), err)
		}
	}
	if rest := idsOf(records[len(published):]); len(rest) > 0 {
		if err := r.store.Release(sctx, rest); err != nil {
			// Not fatal: the events come back when their lease expires.
			r.log.WarnContext(ctx, "release outbox events failed", "err", err)
		}
	}

	r.metrics.published.Add(float64(len(published)))
	if pubErr != nil {
		r.metrics.failed.Inc()
	}
	return len(published), pubErr
}

func (r *Relay) publish(ctx context.Context, rec Record) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	start := time.Now()
	err := r.pub.Publish(ctx, rec)
	r.metrics.duration.Observe(time.Since(start).Seconds())
	return err
}

func idsOf(records []Record) []int64 {
	ids := make([]int64, len(records))
	for i, r := range records {
		ids[i] = r.ID
	}
	return ids
}
