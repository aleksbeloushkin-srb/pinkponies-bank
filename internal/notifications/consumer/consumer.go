// Package consumer reads the transfers.events topic as a member of a Kafka
// consumer group and hands each record to the notifications service.
//
// The rules, in order of importance:
//
//   - An offset is committed only after its record was handled: stored, or
//     found to be a duplicate, or written to the dead-letter topic. A crash
//     at any point redelivers the record, and the handler's deduplication
//     by event id absorbs the repeat. At least once in, exactly once in
//     effect.
//   - Records of a partition are handled one after another, so the events
//     of a transfer (one key, one partition) keep their order.
//   - A poison message (one the handler reports as
//     notifications.ErrInvalidEvent) is retried MaxAttempts times with
//     backoff, then written to the dead-letter topic with headers that say
//     why, and committed. Any other failure (the database is down) is
//     retried with backoff until it works or the consumer stops; it never
//     goes to the dead-letter topic, which would empty the topic into it
//     during an outage.
//   - Rebalances wait while a poll is being handled (BlockRebalanceOnPoll),
//     so a commit never races a partition moving to another member.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
)

// Client is the part of *kgo.Client the consumer needs.
type Client interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// DeadLetters takes records that could not be processed.
type DeadLetters interface {
	Publish(ctx context.Context, r *kgo.Record, cause error, attempts int) error
}

// Handler processes one message.
type Handler interface {
	HandleEvent(ctx context.Context, m app.Message) (outcome string, err error)
}

// Config tunes a Consumer.
type Config struct {
	// MaxPollRecords bounds one poll.
	MaxPollRecords int
	// MaxAttempts is how many times a poison message is tried before it
	// goes to the dead-letter topic.
	MaxAttempts int
	// Backoff spaces the retries of a failing record.
	Backoff taskqueue.Backoff
	// HandleTimeout bounds one call of the handler.
	HandleTimeout time.Duration
	// CommitTimeout bounds one offset commit.
	CommitTimeout time.Duration
}

// Outcomes of consumer_records_total beyond the handler's own.
const outcomeDeadLettered = "dead_lettered"

// Metrics are the consumer's Prometheus metrics.
type Metrics struct {
	records     *prometheus.CounterVec
	retries     *prometheus.CounterVec
	commitFails prometheus.Counter
	duration    prometheus.Histogram
}

// NewMetrics creates the consumer metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_records_total",
			Help: "Records handled, by outcome: stored, duplicate, dead_lettered.",
		}, []string{"outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_retries_total",
			Help: "Failed tries of a record that will be tried again, by class: poison or transient.",
		}, []string{"class"}),
		commitFails: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "consumer_commit_errors_total",
			Help: "Offset commits that failed; the records are delivered again and deduplicated.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "consumer_handle_duration_seconds",
			Help:    "Duration of one successful handling of a record.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
	}
	reg.MustRegister(m.records, m.retries, m.commitFails, m.duration)
	for _, o := range []string{app.OutcomeStored, app.OutcomeDuplicate, outcomeDeadLettered} {
		m.records.WithLabelValues(o)
	}
	for _, c := range []string{"poison", "transient"} {
		m.retries.WithLabelValues(c)
	}
	return m
}

// Consumer runs the poll loop.
type Consumer struct {
	client  Client
	dlq     DeadLetters
	handler Handler
	metrics *Metrics
	cfg     Config
	log     *slog.Logger
	// sleep waits between retries; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
}

// New returns a Consumer.
func New(client Client, dlq DeadLetters, handler Handler, m *Metrics, cfg Config, log *slog.Logger) *Consumer {
	return &Consumer{client: client, dlq: dlq, handler: handler, metrics: m, cfg: cfg, log: log, sleep: sleepCtx}
}

// Run polls and handles records until ctx is cancelled. The record being
// retried when ctx ends is not committed; it comes back to whichever member
// owns its partition next.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
		if ctx.Err() != nil {
			c.client.AllowRebalance()
			return nil
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.WarnContext(ctx, "fetch failed", "topic", topic, "partition", partition, "err", err)
		})

		var (
			handled []*kgo.Record
			stopped bool
		)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				if stopped {
					return
				}
				if err := c.process(ctx, r); err != nil {
					stopped = true // shutting down; this record is not handled
					return
				}
				handled = append(handled, r)
			}
		})
		c.commit(ctx, handled)
		c.client.AllowRebalance()
		if stopped {
			return nil
		}
	}
}

// process handles one record, retrying until it is handled or ctx ends.
// It returns an error only when ctx ends first.
func (c *Consumer) process(ctx context.Context, r *kgo.Record) error {
	msg := app.Message{
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
		Key: r.Key, Value: r.Value, EventID: header(r, eventsv1.HeaderEventID),
	}
	for attempt := 1; ; attempt++ {
		start := time.Now()
		hctx, cancel := context.WithTimeout(ctx, c.cfg.HandleTimeout)
		outcome, err := c.handler.HandleEvent(hctx, msg)
		cancel()
		if err == nil {
			c.metrics.duration.Observe(time.Since(start).Seconds())
			c.metrics.records.WithLabelValues(outcome).Inc()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		poison := errors.Is(err, notifications.ErrInvalidEvent)
		attrs := []any{"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "attempt", attempt, "err", err}
		if poison && attempt >= c.cfg.MaxAttempts {
			return c.deadLetter(ctx, r, err, attempt)
		}
		class := "transient"
		if poison {
			class = "poison"
		}
		c.metrics.retries.WithLabelValues(class).Inc()
		delay := c.cfg.Backoff.Delay(attempt)
		c.log.WarnContext(ctx, "record failed, will retry", append(attrs, "class", class, "retry_in", delay.String())...)
		if err := c.sleep(ctx, delay); err != nil {
			return err
		}
	}
}

// deadLetter writes a poison record to the dead-letter topic, retrying the
// write itself until it works or ctx ends: the record must not be committed
// before it is safely somewhere.
func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error, attempts int) error {
	for try := 1; ; try++ {
		err := c.dlq.Publish(ctx, r, cause, attempts)
		if err == nil {
			c.metrics.records.WithLabelValues(outcomeDeadLettered).Inc()
			c.log.ErrorContext(ctx, "record moved to the dead-letter topic",
				"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "attempts", attempts, "cause", cause)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.WarnContext(ctx, "dead-letter write failed, will retry", "err", err)
		if err := c.sleep(ctx, c.cfg.Backoff.Delay(try)); err != nil {
			return err
		}
	}
}

// commit commits the handled records. It runs even when ctx was cancelled:
// the records were handled, and committing them spares a redelivery. A
// failed commit is not fatal: the records come back and are deduplicated.
func (c *Consumer) commit(ctx context.Context, rs []*kgo.Record) {
	if len(rs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.CommitTimeout)
	defer cancel()
	if err := c.client.CommitRecords(cctx, rs...); err != nil {
		c.metrics.commitFails.Inc()
		c.log.WarnContext(ctx, "commit failed; the records will be delivered again", "records", len(rs), "err", err)
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Options are the client options of a consumer group member that commits
// by hand and blocks rebalances during a poll. A new group starts from the
// beginning of the topic, so no event published before it existed is
// skipped.
func Options(group string, topics ...string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// The dead-letter producer shares the client.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	}
}

// DeadLetterTopic writes poison records to a topic with the producer of
// the consumer's client.
type DeadLetterTopic struct {
	producer interface {
		ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	}
	topic string
}

// NewDeadLetterTopic returns a DeadLetters that writes to topic.
func NewDeadLetterTopic(producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}, topic string) *DeadLetterTopic {
	return &DeadLetterTopic{producer: producer, topic: topic}
}

// Dead-letter headers, added to the original record's headers.
const (
	HeaderDLQError             = "dlq-error"
	HeaderDLQAttempts          = "dlq-attempts"
	HeaderDLQOriginalTopic     = "dlq-original-topic"
	HeaderDLQOriginalPartition = "dlq-original-partition"
	HeaderDLQOriginalOffset    = "dlq-original-offset"
)

// maxErrorHeader truncates the cause in the headers.
const maxErrorHeader = 500

// Publish copies r to the dead-letter topic with the same key and value,
// its headers, and headers that say where it came from and why.
func (d *DeadLetterTopic) Publish(ctx context.Context, r *kgo.Record, cause error, attempts int) error {
	msg := cause.Error()
	if len(msg) > maxErrorHeader {
		msg = msg[:maxErrorHeader]
	}
	headers := append([]kgo.RecordHeader{}, r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(msg)},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: fmt.Appendf(nil, "%d", attempts)},
		kgo.RecordHeader{Key: HeaderDLQOriginalTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: HeaderDLQOriginalPartition, Value: fmt.Appendf(nil, "%d", r.Partition)},
		kgo.RecordHeader{Key: HeaderDLQOriginalOffset, Value: fmt.Appendf(nil, "%d", r.Offset)},
	)
	rec := &kgo.Record{Topic: d.topic, Key: r.Key, Value: r.Value, Headers: headers}
	if err := d.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", d.topic, err)
	}
	return nil
}
