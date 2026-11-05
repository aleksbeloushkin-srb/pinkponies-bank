// Package metrics implements the ledger's business measurements
// (app.Metrics) with Prometheus, on a registry passed in by the composition
// root.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
)

// Business counts holds placed, captured and released. Replays of an
// idempotency key are not counted.
type Business struct {
	placed   prometheus.Counter
	captured prometheus.Counter
	released *prometheus.CounterVec
}

// NewBusiness creates the business metrics and registers them on reg.
func NewBusiness(reg prometheus.Registerer) *Business {
	b := &Business{
		placed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ledger_holds_placed_total",
			Help: "Holds placed. Replays of an idempotency key are not counted.",
		}),
		captured: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ledger_holds_captured_total",
			Help: "Holds captured, each one a balanced transfer transaction.",
		}),
		released: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ledger_holds_released_total",
			Help: "Holds released, by cause: requested (compensation) or expired (the sweep).",
		}, []string{"cause"}),
	}
	reg.MustRegister(b.placed, b.captured, b.released)
	for _, cause := range []string{app.CauseRequested, app.CauseExpired} {
		b.released.WithLabelValues(cause)
	}
	return b
}

// HoldPlaced counts a placed hold.
func (b *Business) HoldPlaced() { b.placed.Inc() }

// HoldCaptured counts a captured hold.
func (b *Business) HoldCaptured() { b.captured.Inc() }

// HoldReleased counts a released hold by cause.
func (b *Business) HoldReleased(cause string) { b.released.WithLabelValues(cause).Inc() }
