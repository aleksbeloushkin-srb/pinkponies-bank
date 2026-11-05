package pgdb

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// PoolCollector exports the connection pool's statistics. Reading them takes
// no query, so it cannot fail.
type PoolCollector struct {
	pool *pgxpool.Pool

	acquired, idle, total, max *prometheus.Desc
	acquires, emptyAcquires    *prometheus.Desc
	acquireWait                *prometheus.Desc
}

// NewPoolCollector returns a collector over pool.
func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	desc := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	return &PoolCollector{
		pool:          pool,
		acquired:      desc("db_pool_acquired_conns", "Connections currently in use."),
		idle:          desc("db_pool_idle_conns", "Idle connections in the pool."),
		total:         desc("db_pool_total_conns", "Open connections, in use or idle."),
		max:           desc("db_pool_max_conns", "Maximum size of the pool."),
		acquires:      desc("db_pool_acquires_total", "Successful connection acquisitions."),
		emptyAcquires: desc("db_pool_empty_acquires_total", "Acquisitions that had to wait because the pool was empty."),
		acquireWait:   desc("db_pool_acquire_wait_seconds_total", "Total time spent acquiring connections."),
	}
}

// Describe sends the descriptors of every pool metric.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.acquired, c.idle, c.total, c.max, c.acquires, c.emptyAcquires, c.acquireWait} {
		ch <- d
	}
}

// Collect reads pgxpool.Stat and sends the values.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquires, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireWait, prometheus.CounterValue, s.AcquireDuration().Seconds())
}
