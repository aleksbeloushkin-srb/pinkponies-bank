package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// Collector exports gauges read from the ledger database on every scrape:
// active holds, the age of the oldest one, and the sum of posted balances
// per currency and account kind.
//
// The values describe the whole database, so every instance exports the
// same numbers: aggregate them with max(), not sum(). When the query fails,
// the collector counts the failure and emits no gauges for that scrape; a
// stale "0 active holds" would look healthy while the database is down.
type Collector struct {
	db      txctx.Querier
	timeout time.Duration
	log     *slog.Logger

	holdsActive *prometheus.Desc
	oldestHold  *prometheus.Desc
	balanceSum  *prometheus.Desc
	errors      prometheus.Counter
}

// NewCollector returns a collector that queries db, each scrape bounded by
// timeout.
func NewCollector(db txctx.Querier, timeout time.Duration, log *slog.Logger) *Collector {
	return &Collector{
		db:      db,
		timeout: timeout,
		log:     log,
		holdsActive: prometheus.NewDesc("ledger_holds_active",
			"Holds in the active state.", nil, nil),
		oldestHold: prometheus.NewDesc("ledger_hold_oldest_active_seconds",
			"Age of the oldest active hold; 0 when none. A hold that outlives its transfer is a leak.", nil, nil),
		balanceSum: prometheus.NewDesc("ledger_balance_sum_minor",
			"Sum of posted balances in minor units by currency and account kind. Per currency, customer plus system must be 0.",
			[]string{"currency", "kind"}, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "metrics_collect_errors_total",
			Help: "Scrapes in which the database gauges could not be read.",
		}),
	}
}

// Describe sends the descriptors of every metric the collector can emit.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.holdsActive
	ch <- c.oldestHold
	ch <- c.balanceSum
	c.errors.Describe(ch)
}

// statsSQL returns one row per value. The balance sum scans every account;
// see docs/REVIEW.md for when that stops being acceptable.
const statsSQL = `
	SELECT 'holds_active' AS name, '' AS currency, '' AS kind, count(*)::float8 AS value
	FROM holds WHERE status = 'active'
	UNION ALL
	SELECT 'oldest_hold', '', '', greatest(coalesce(extract(epoch FROM now() - min(created_at))::float8, 0), 0)
	FROM holds WHERE status = 'active'
	UNION ALL
	SELECT 'balance', currency, kind, sum(posted_minor)::float8
	FROM accounts GROUP BY currency, kind`

type balanceKey struct{ currency, kind string }

// Collect queries the database and sends the gauges. Prometheus gives
// Collect no context, so the query gets its own timeout.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	defer c.errors.Collect(ch)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	active, oldest, balances, err := c.query(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("collect ledger metrics failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.holdsActive, prometheus.GaugeValue, active)
	ch <- prometheus.MustNewConstMetric(c.oldestHold, prometheus.GaugeValue, oldest)
	for k, v := range balances {
		ch <- prometheus.MustNewConstMetric(c.balanceSum, prometheus.GaugeValue, v, k.currency, k.kind)
	}
}

func (c *Collector) query(ctx context.Context) (active, oldest float64, balances map[balanceKey]float64, err error) {
	rows, err := c.db.Query(ctx, statsSQL)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("query ledger stats: %w", err)
	}
	defer rows.Close()
	balances = map[balanceKey]float64{}
	for rows.Next() {
		var (
			name, currency, kind string
			value                float64
		)
		if err := rows.Scan(&name, &currency, &kind, &value); err != nil {
			return 0, 0, nil, fmt.Errorf("scan ledger stats: %w", err)
		}
		switch name {
		case "holds_active":
			active = value
		case "oldest_hold":
			oldest = value
		case "balance":
			balances[balanceKey{currency, kind}] = value
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, fmt.Errorf("read ledger stats: %w", err)
	}
	return active, oldest, balances, nil
}
