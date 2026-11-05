package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	described, err := c.adm.Lag(ctx, c.group)
	if err != nil {
		return nil, nil, fmt.Errorf("lag: %w", err)
	}
	lags = map[string]int64{}
	if g, ok := described[c.group]; ok {
		if err := g.Error(); err != nil {
			return nil, nil, fmt.Errorf("lag of %s: %w", c.group, err)
		}
		for topic, l := range g.Lag.TotalByTopic() {
			lags[topic] = l.Lag
		}
	}

	start, err := c.adm.ListStartOffsets(ctx, c.sized...)
	if err == nil {
		err = start.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("start offsets: %w", err)
	}
	end, err := c.adm.ListEndOffsets(ctx, c.sized...)
	if err == nil {
		err = end.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("end offsets: %w", err)
	}
	sizes = map[string]int64{}
	var errs []error
	end.Each(func(o kadm.ListedOffset) {
		s, ok := start.Lookup(o.Topic, o.Partition)
		if !ok {
			errs = append(errs, fmt.Errorf("no start offset for %s/%d", o.Topic, o.Partition))
			return
		}
		sizes[o.Topic] += max(o.Offset-s.Offset, 0)
	})
	return lags, sizes, errors.Join(errs...)
}
