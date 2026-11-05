// Package metrics implements the transfers service's business
// measurements (app.Metrics) with Prometheus, on a registry passed in by
// the composition root.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// Business counts transfers, their status changes and their saga step
// outcomes. Only committed changes are reported.
type Business struct {
	created     prometheus.Counter
	transitions *prometheus.CounterVec
	timeToFinal *prometheus.HistogramVec
	steps       *prometheus.CounterVec
}

// NewBusiness creates the business metrics and registers them on reg.
func NewBusiness(reg prometheus.Registerer) *Business {
	b := &Business{
		created: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "transfers_created_total",
			Help: "Transfers accepted. A replay of an Idempotency-Key is not counted.",
		}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "transfer_transitions_total",
			Help: "Committed transfer status changes.",
		}, []string{"from", "to"}),
		timeToFinal: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "transfer_time_to_final_seconds",
			Help: "Time from acceptance to a final status.",
			// From an instant refusal to a long review; fine in the first
			// seconds, where most transfers end.
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60, 120, 300, 900},
		}, []string{"status"}),
		steps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_steps_finished_total",
			Help: "Committed saga step outcomes by step and outcome.",
		}, []string{"step", "outcome"}),
	}
	reg.MustRegister(b.created, b.transitions, b.timeToFinal, b.steps)
	// Every series at zero, so increase() sees the first event.
	for step, outcomes := range transfers.StepOutcomes {
		for _, o := range outcomes {
			b.steps.WithLabelValues(string(step), o)
		}
	}
	return b
}

// TransferCreated counts an accepted transfer.
func (b *Business) TransferCreated() { b.created.Inc() }

// StatusChanged counts a transition and, for a final status, observes how
// long the transfer took.
func (b *Business) StatusChanged(from, to transfers.Status, age time.Duration) {
	b.transitions.WithLabelValues(string(from), string(to)).Inc()
	if to.Final() {
		b.timeToFinal.WithLabelValues(string(to)).Observe(age.Seconds())
	}
}

// StepFinished counts a step outcome.
func (b *Business) StepFinished(step transfers.Step, outcome string) {
	b.steps.WithLabelValues(string(step), outcome).Inc()
}
