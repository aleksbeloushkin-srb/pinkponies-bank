// Package metrics implements the notifications service's business
// measurements (app.Metrics) with Prometheus.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
)

// Business counts notifications and deliveries.
type Business struct {
	created    *prometheus.CounterVec
	deliveries *prometheus.CounterVec
}

// NewBusiness creates the metrics and registers them on reg, with the
// known label values at zero.
func NewBusiness(reg prometheus.Registerer, channels []string) *Business {
	b := &Business{
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications created, by kind. A duplicate event creates none.",
		}, []string{"kind"}),
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "deliveries_total",
			Help: "Delivery attempts by channel and outcome: delivered, failed (will retry), gave_up.",
		}, []string{"channel", "outcome"}),
	}
	reg.MustRegister(b.created, b.deliveries)
	for _, k := range notifications.Kinds {
		b.created.WithLabelValues(string(k))
	}
	for _, ch := range channels {
		for _, o := range []string{"delivered", "failed", "gave_up"} {
			b.deliveries.WithLabelValues(ch, o)
		}
	}
	return b
}

// NotificationCreated counts a stored notification.
func (b *Business) NotificationCreated(kind notifications.Kind) {
	b.created.WithLabelValues(string(kind)).Inc()
}

// Delivered counts one attempt.
func (b *Business) Delivered(channel string, ok bool) {
	outcome := "failed"
	if ok {
		outcome = "delivered"
	}
	b.deliveries.WithLabelValues(channel, outcome).Inc()
}

// GaveUp counts a delivery abandoned after its attempts.
func (b *Business) GaveUp(channel string) {
	b.deliveries.WithLabelValues(channel, "gave_up").Inc()
}
