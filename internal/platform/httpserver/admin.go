package httpserver

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewAdminHandler serves the operator endpoints of the admin listener:
// Prometheus metrics, liveness and readiness. The admin port is meant for
// the cluster network (the scraper, the orchestrator's probes), never for
// clients.
func NewAdminHandler(metrics prometheus.Gatherer, ready func() bool, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics, promhttp.HandlerOpts{
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelError),
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// readyz turns 503 as soon as shutdown starts, so the load balancer
	// stops sending new requests before the server stops accepting them.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}

// ServerConfig holds the server timeouts. All four must be set: the zero
// value of each means "no limit".
type ServerConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	// WriteTimeout must exceed any long-poll soft timeout, or the server
	// cuts the response off before it is written.
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// NewServer returns an http.Server for h.
func NewServer(cfg ServerConfig, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

// AdminServerConfig is the configuration of every admin listener: a scrape
// or a probe takes milliseconds.
func AdminServerConfig(addr string) ServerConfig {
	return ServerConfig{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// AdminShutdownTimeout bounds the drain of an admin listener.
const AdminShutdownTimeout = 5 * time.Second
