package taskqueue

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Attempt outcomes recorded in task_attempts_total.
const (
	outcomeOK        = "ok"
	outcomeRetry     = "retry"
	outcomePermanent = "permanent"
	outcomeDead      = "dead"
)

var outcomes = []string{outcomeOK, outcomeRetry, outcomePermanent, outcomeDead}

// Metrics are the queue's Prometheus metrics. Labels are the task kind and
// the outcome, both from small fixed sets; task and entity ids never become
// labels.
type Metrics struct {
	claimed   *prometheus.CounterVec
	attempts  *prometheus.CounterVec
	lag       *prometheus.HistogramVec
	duration  *prometheus.HistogramVec
	leaseLost prometheus.Counter
	panics    prometheus.Counter
}

// NewMetrics creates the queue metrics and registers them on reg. kinds
// are created at zero, so increase() and rate() see the first event
// instead of starting from it.
func NewMetrics(reg prometheus.Registerer, kinds []string) *Metrics {
	m := &Metrics{
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_claimed_total",
			Help: "Tasks claimed by this worker.",
		}, []string{"kind"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_attempts_total",
			Help: "Settled task runs by outcome: ok (done or postponed), retry, permanent (dead at once), dead (out of attempts).",
		}, []string{"kind", "outcome"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_lag_seconds",
			Help:    "Time between a task becoming due (run_after) and its claim.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}, []string{"kind"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_duration_seconds",
			Help:    "Run time of a task handler, including remote calls and row-lock waits.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"kind"}),
		leaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lease_lost_total",
			Help: "Task results discarded because the lease had passed to another worker.",
		}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_panics_total",
			Help: "Task handler panics recovered by the worker.",
		}),
	}
	reg.MustRegister(m.claimed, m.attempts, m.lag, m.duration, m.leaseLost, m.panics)
	for _, kind := range kinds {
		m.claimed.WithLabelValues(kind)
		for _, o := range outcomes {
			m.attempts.WithLabelValues(kind, o)
		}
	}
	return m
}
