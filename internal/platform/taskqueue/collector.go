package taskqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// Collector exports gauges about the task table, read on every scrape with
// one query bounded by a timeout.
//
// The values describe the whole database, so every instance of a service
// exports the same numbers: aggregate them with max(), not sum(). When the
// query fails, the collector increments task_collect_errors_total and emits
// no gauges for that scrape: a frozen tasks_dead of 0 would look healthy
// while the database is unreachable.
type Collector struct {
	db      txctx.Querier
	kinds   []string
	timeout time.Duration
	log     *slog.Logger

	ready       *prometheus.Desc
	dead        *prometheus.Desc
	oldestReady *prometheus.Desc
	errors      prometheus.Counter
}

// NewCollector returns a collector for the given task kinds, which are
// always exported, at 0 if absent, so series do not come and go with the
// workload.
func NewCollector(db txctx.Querier, kinds []string, timeout time.Duration, log *slog.Logger) *Collector {
	return &Collector{
		db:      db,
		kinds:   kinds,
		timeout: timeout,
		log:     log,
		ready: prometheus.NewDesc("tasks_ready",
			"Tasks in the ready state by kind, including those scheduled for later.", []string{"kind"}, nil),
		dead: prometheus.NewDesc("tasks_dead",
			"Tasks in the dead state by kind, waiting for an operator.", []string{"kind"}, nil),
		oldestReady: prometheus.NewDesc("task_oldest_ready_seconds",
			"Age of the oldest task that is due but not yet done; 0 when none.", nil, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_collect_errors_total",
			Help: "Scrapes in which the task gauges could not be read.",
		}),
	}
}

// Describe sends the descriptors of every metric the collector can emit.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ready
	ch <- c.dead
	ch <- c.oldestReady
	c.errors.Describe(ch)
}

const statsSQL = `
	SELECT 'ready' AS name, kind, count(*)::float8 AS value
	FROM tasks WHERE state = 'ready' GROUP BY kind
	UNION ALL
	SELECT 'dead', kind, count(*)::float8
	FROM tasks WHERE state = 'dead' GROUP BY kind
	UNION ALL
	SELECT 'oldest_ready', '', coalesce(extract(epoch FROM now() - min(run_after))::float8, 0)
	FROM tasks WHERE state = 'ready' AND run_after <= now()`

// Collect queries the database and sends the gauges. Prometheus gives
// Collect no context, so the query gets its own timeout.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	defer c.errors.Collect(ch)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	ready, dead, oldest, err := c.query(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("collect task metrics failed", "err", err)
		return
	}
	for _, kind := range c.kinds {
		ch <- prometheus.MustNewConstMetric(c.ready, prometheus.GaugeValue, ready[kind], kind)
		ch <- prometheus.MustNewConstMetric(c.dead, prometheus.GaugeValue, dead[kind], kind)
	}
	ch <- prometheus.MustNewConstMetric(c.oldestReady, prometheus.GaugeValue, max(oldest, 0))
}

func (c *Collector) query(ctx context.Context) (ready, dead map[string]float64, oldest float64, err error) {
	rows, err := c.db.Query(ctx, statsSQL)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("query task stats: %w", err)
	}
	defer rows.Close()
	ready, dead = map[string]float64{}, map[string]float64{}
	for rows.Next() {
		var (
			name, kind string
			value      float64
		)
		if err := rows.Scan(&name, &kind, &value); err != nil {
			return nil, nil, 0, fmt.Errorf("scan task stats: %w", err)
		}
		switch name {
		case "ready":
			ready[kind] = value
		case "dead":
			dead[kind] = value
		case "oldest_ready":
			oldest = value
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, fmt.Errorf("read task stats: %w", err)
	}
	return ready, dead, oldest, nil
}
