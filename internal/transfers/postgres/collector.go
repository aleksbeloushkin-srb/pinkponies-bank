package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// Collector exports saga and outbox gauges read from the database on every
// scrape: transfers in flight by status, the age of the oldest one, and the
// age of the oldest unpublished event.
//
// The values describe the whole database: aggregate them with max(), not
// sum(). On a failed query the collector counts the failure and emits no
// gauges for that scrape, so nothing stale looks healthy.
type Collector struct {
	db      txctx.Querier
	timeout time.Duration
	log     *slog.Logger

	inFlight          *prometheus.Desc
	oldestInFlight    *prometheus.Desc
	oldestUnpublished *prometheus.Desc
	errors            prometheus.Counter
}

// NewCollector returns a collector that queries db, each scrape bounded by
// timeout.
func NewCollector(db txctx.Querier, timeout time.Duration, log *slog.Logger) *Collector {
	return &Collector{
		db:      db,
		timeout: timeout,
		log:     log,
		inFlight: prometheus.NewDesc("transfers_in_flight",
			"Transfers that are not final, by status.", []string{"status"}, nil),
		oldestInFlight: prometheus.NewDesc("transfer_oldest_in_flight_seconds",
			"Age of the oldest transfer that is not final; 0 when none. A saga stuck in a step makes it grow.", nil, nil),
		oldestUnpublished: prometheus.NewDesc("outbox_oldest_unpublished_seconds",
			"Age of the oldest unpublished outbox event; 0 when none.", nil, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "metrics_collect_errors_total",
			Help: "Scrapes in which the saga and outbox gauges could not be read.",
		}),
	}
}

// Describe sends the descriptors of every metric the collector can emit.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inFlight
	ch <- c.oldestInFlight
	ch <- c.oldestUnpublished
	c.errors.Describe(ch)
}

const statsSQL = `
	SELECT 'in_flight' AS name, status, count(*)::float8 AS value
	FROM transfers WHERE status IN ('pending', 'held') GROUP BY status
	UNION ALL
	SELECT 'oldest_in_flight', '', greatest(coalesce(extract(epoch FROM now() - min(created_at))::float8, 0), 0)
	FROM transfers WHERE status IN ('pending', 'held')
	UNION ALL
	SELECT 'oldest_unpublished', '', greatest(coalesce(extract(epoch FROM now() - min(created_at))::float8, 0), 0)
	FROM outbox WHERE published_at IS NULL`

// Collect queries the database and sends the gauges.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	defer c.errors.Collect(ch)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	stats, err := c.query(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("collect saga metrics failed", "err", err)
		return
	}
	for _, status := range []transfers.Status{transfers.StatusPending, transfers.StatusHeld} {
		ch <- prometheus.MustNewConstMetric(c.inFlight, prometheus.GaugeValue, stats["in_flight:"+string(status)], string(status))
	}
	ch <- prometheus.MustNewConstMetric(c.oldestInFlight, prometheus.GaugeValue, stats["oldest_in_flight:"])
	ch <- prometheus.MustNewConstMetric(c.oldestUnpublished, prometheus.GaugeValue, stats["oldest_unpublished:"])
}

func (c *Collector) query(ctx context.Context) (map[string]float64, error) {
	rows, err := c.db.Query(ctx, statsSQL)
	if err != nil {
		return nil, fmt.Errorf("query saga stats: %w", err)
	}
	defer rows.Close()
	stats := map[string]float64{}
	for rows.Next() {
		var (
			name, label string
			value       float64
		)
		if err := rows.Scan(&name, &label, &value); err != nil {
			return nil, fmt.Errorf("scan saga stats: %w", err)
		}
		stats[name+":"+label] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read saga stats: %w", err)
	}
	return stats, nil
}
