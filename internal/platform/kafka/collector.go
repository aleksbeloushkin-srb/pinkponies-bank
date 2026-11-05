package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Collector exports, on every scrape, the lag of a consumer group on the
// topics it consumes and the number of messages retained in a set of
// topics (end offset minus start offset, summed over partitions). The
// second is how a dead-letter topic that is not empty becomes an alert.
//
// The numbers come from the brokers, so every instance of a service exports
// the same values: aggregate with max(), not sum(). A failed scrape counts
// kafka_collect_errors_total and emits no gauges, never stale ones.
type Collector struct {
	adm      *kadm.Client
	group    string
	consumed []string
	sized    []string
	timeout  time.Duration
	log      *slog.Logger

	lag      *prometheus.Desc
	messages *prometheus.Desc
	errors   prometheus.Counter
}

// NewCollector returns a collector of group's lag on consumed and of the
// size of sized, each scrape bounded by timeout.
func NewCollector(adm *kadm.Client, group string, consumed, sized []string, timeout time.Duration, log *slog.Logger) *Collector {
	return &Collector{
		adm: adm, group: group, consumed: consumed, sized: sized, timeout: timeout, log: log,
		lag: prometheus.NewDesc("kafka_consumer_group_lag",
			"Messages not yet committed by the consumer group, summed over the topic's partitions.", []string{"group", "topic"}, nil),
		messages: prometheus.NewDesc("kafka_topic_messages",
			"Messages retained in the topic: end offset minus start offset, summed over partitions.", []string{"topic"}, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_collect_errors_total",
			Help: "Scrapes in which consumer lag or topic sizes could not be read.",
		}),
	}
}

// Describe sends the descriptors of every metric the collector can emit.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
	ch <- c.messages
	c.errors.Describe(ch)
}

// Collect reads the lag and the topic sizes and sends the gauges.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	defer c.errors.Collect(ch)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	lags, sizes, err := c.read(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("collect kafka metrics failed", "err", err)
		return
	}
	for _, topic := range c.consumed {
		ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, float64(lags[topic]), c.group, topic)
	}
	for _, topic := range c.sized {
		ch <- prometheus.MustNewConstMetric(c.messages, prometheus.GaugeValue, float64(sizes[topic]), topic)
	}
}

func (c *Collector) read(ctx context.Context) (lags, sizes map[string]int64, err error) {
	committed, err := c.adm.FetchOffsets(ctx, c.group)
	if err != nil {
		return nil, nil, fmt.Errorf("committed offsets of %s: %w", c.group, err)
	}
	topics := append(slices.Clone(c.consumed), c.sized...)
	start, err := c.adm.ListStartOffsets(ctx, topics...)
	if err == nil {
		err = start.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("start offsets: %w", err)
	}
	end, err := c.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = end.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("end offsets: %w", err)
	}
	var partitions []partitionOffsets
	var errs []error
	end.Each(func(o kadm.ListedOffset) {
		s, ok := start.Lookup(o.Topic, o.Partition)
		if !ok {
			errs = append(errs, fmt.Errorf("no start offset for %s/%d", o.Topic, o.Partition))
			return
		}
		p := partitionOffsets{topic: o.Topic, start: s.Offset, end: o.Offset, committed: -1}
		if r, ok := committed.Lookup(o.Topic, o.Partition); ok {
			if r.Err != nil {
				errs = append(errs, fmt.Errorf("committed offset of %s/%d: %w", o.Topic, o.Partition, r.Err))
				return
			}
			p.committed = r.At
		}
		partitions = append(partitions, p)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}
	lags, sizes = summarize(partitions)
	return lags, sizes, nil
}

// partitionOffsets are the offsets of one partition; committed is -1 when
// the group never committed it.
type partitionOffsets struct {
	topic                 string
	start, end, committed int64
}

// summarize sums lag and size per topic. The lag of a partition is what
// lies between the group's commit and the end; without a commit, between
// the start and the end, because a new group starts from the beginning.
// kadm's own Lag skips partitions without a commit when the group has no
// members, which is exactly when the consumer is down: computing it here
// keeps that case visible.
func summarize(partitions []partitionOffsets) (lags, sizes map[string]int64) {
	lags, sizes = map[string]int64{}, map[string]int64{}
	for _, p := range partitions {
		from := p.start
		if p.committed >= 0 {
			from = max(p.committed, p.start)
		}
		lags[p.topic] += max(p.end-from, 0)
		sizes[p.topic] += max(p.end-p.start, 0)
	}
	return lags, sizes
}
