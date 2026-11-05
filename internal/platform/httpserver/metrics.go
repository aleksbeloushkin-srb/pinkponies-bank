package httpserver

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are an API's request metrics (rate, errors, duration).
//
// Labels stay low-cardinality: route is the ServeMux pattern, never the raw
// path (which contains ids); method is folded into a fixed set; code is the
// status code.
type Metrics struct {
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	longPoll  prometheus.Histogram
	longPolls []string
}

// NewMetrics creates the HTTP metrics and registers them on reg.
// longPollPatterns are the ServeMux patterns of routes that are slow by
// design; their durations go to a separate histogram.
func NewMetrics(reg prometheus.Registerer, longPollPatterns ...string) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route pattern, method and status code.",
		}, []string{"route", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration by route pattern and method, long polls excluded.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		longPoll: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "http_long_poll_duration_seconds",
			Help:    "Duration of long-poll requests, which are long by design.",
			Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 15, 20, 25, 30},
		}),
		longPolls: longPollPatterns,
	}
	reg.MustRegister(m.requests, m.duration, m.longPoll)
	return m
}

// Instrument records every request in http_requests_total and its duration
// in one of two histograms.
//
// A long poll lasts up to its soft timeout on purpose, so its duration says
// nothing about how fast the service is; mixed into
// http_request_duration_seconds it would dominate any p99 taken across
// routes. Its count and status codes stay in http_requests_total.
//
// Instrument sits outside Recoverer, so a recovered panic is counted as the
// 500 it became.
func Instrument(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			elapsed := time.Since(start).Seconds()

			route, method := routeLabel(r.Pattern), methodLabel(r.Method)
			m.requests.WithLabelValues(route, method, strconv.Itoa(sw.status)).Inc()
			if slices.Contains(m.longPolls, r.Pattern) {
				m.longPoll.Observe(elapsed)
				return
			}
			m.duration.WithLabelValues(route, method).Observe(elapsed)
		})
	}
}

// routeLabel turns a ServeMux pattern such as "GET /v1/transfers/{id}" into
// "/v1/transfers/{id}". Requests that matched no route share one label, so
// a scan of random paths cannot create new series.
func routeLabel(pattern string) string {
	if pattern == "" {
		return "unmatched"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

// methodLabel keeps standard methods and folds anything else, which a
// client can invent freely, into "OTHER".
func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	}
	return "OTHER"
}
