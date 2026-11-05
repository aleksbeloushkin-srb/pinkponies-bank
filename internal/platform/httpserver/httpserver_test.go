package httpserver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(buf)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", sc.Text())
		}
		out = append(out, m)
	}
	return out
}

// The panic line and the access line carry the same request id, and the
// access log records the 500 the panic became.
func TestMiddlewareOrder(t *testing.T) {
	var logs bytes.Buffer
	log := platform.NewLogger(&logs, slog.LevelInfo)
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("nil pointer") }),
		RequestID, Logging(log), Recoverer(log))

	rec := do(h, httptest.NewRequest(http.MethodGet, "/boom", nil))

	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != http.StatusInternalServerError || body.Code != "internal" {
		t.Fatalf("response = %d %s", rec.Code, rec.Body)
	}
	id := rec.Header().Get(RequestIDHeader)
	lines := logLines(t, &logs)
	if id == "" || len(lines) != 2 {
		t.Fatalf("request id %q, log lines %v", id, lines)
	}
	if lines[0]["msg"] != "panic recovered" || lines[0]["request_id"] != id {
		t.Fatalf("panic line = %v", lines[0])
	}
	if lines[1]["msg"] != "http request" || lines[1]["status"] != float64(500) || lines[1]["request_id"] != id {
		t.Fatalf("access line = %v", lines[1])
	}
}

func TestRequestIDIsKeptOnlyWhenWellFormed(t *testing.T) {
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if platform.RequestID(r.Context()) != w.Header().Get(RequestIDHeader) {
			t.Error("context and response must carry the same id")
		}
	}))
	for incoming, keep := range map[string]bool{"abc-123": true, "bad id with spaces": false, "": false} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(RequestIDHeader, incoming)
		got := do(h, req).Header().Get(RequestIDHeader)
		if (got == incoming) != keep || got == "" {
			t.Errorf("incoming %q -> %q, keep %v", incoming, got, keep)
		}
	}
}

func TestDecodeJSON(t *testing.T) {
	type in struct {
		Name string `json:"name"`
	}
	tests := []struct {
		name, body string
		check      func(error) bool
	}{
		{"ok", `{"name":"pip"}`, func(err error) bool { return err == nil }},
		{"unknown field", `{"name":"pip","nmae":"x"}`, isBodyError},
		{"two objects", `{"name":"a"}{"name":"b"}`, isBodyError},
		{"not JSON", `name=pip`, isBodyError},
		{"too large", `{"name":"` + strings.Repeat("a", 100) + `"}`, func(err error) bool {
			var maxErr *http.MaxBytesError
			return errors.As(err, &maxErr)
		}},
	}
	for _, tt := range tests {
		var dst in
		rec := httptest.NewRecorder()
		err := DecodeJSON(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body)), 64, &dst)
		if !tt.check(err) {
			t.Errorf("%s: err = %v", tt.name, err)
		}
	}
}

func isBodyError(err error) bool {
	var be *BodyError
	return errors.As(err, &be)
}

func TestAdminHandler(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg).requests.WithLabelValues("/v1/transfers", "POST", "201").Inc()
	var ready atomic.Bool
	h := NewAdminHandler(reg, ready.Load, discard())

	rec := do(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `http_requests_total{code="201",method="POST",route="/v1/transfers"} 1`) {
		t.Fatalf("GET /metrics = %d\n%s", rec.Code, rec.Body)
	}
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/healthz", nil)); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil)); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before start = %d, want 503", rec.Code)
	}
	ready.Store(true)
	if rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil)); rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d, want 200", rec.Code)
	}
}

func TestRequestMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	const wait = "GET /v1/transfers/{id}/wait"
	m := NewMetrics(reg, wait)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/transfers/{id}", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc(wait, func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("POST /v1/transfers", func(http.ResponseWriter, *http.Request) {})
	h := Chain(mux, Instrument(m))

	do(h, httptest.NewRequest(http.MethodGet, "/v1/transfers/a", nil))
	do(h, httptest.NewRequest(http.MethodGet, "/v1/transfers/b", nil))
	do(h, httptest.NewRequest(http.MethodGet, "/v1/transfers/a/wait", nil))
	do(h, httptest.NewRequest("BREW", "/v1/transfers", nil))

	if got := testutil.ToFloat64(m.requests.WithLabelValues("/v1/transfers/{id}", "GET", "200")); got != 2 {
		t.Fatalf("requests for the transfer route = %v, want 2 under one series", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("/v1/transfers/{id}/wait", "GET", "200")); got != 1 {
		t.Fatalf("long polls counted = %v, want 1", got)
	}
	// The path exists for POST only, so ServeMux answers 405 without a
	// pattern.
	if got := testutil.ToFloat64(m.requests.WithLabelValues("unmatched", "OTHER", "405")); got != 1 {
		t.Fatalf("unknown method = %v, want it folded into OTHER / unmatched", got)
	}
	if n := testutil.CollectAndCount(m.duration); n != 2 {
		t.Fatalf("latency series = %d, want 2 (the transfer route and unmatched)", n)
	}
	var lp dto.Metric
	if err := m.longPoll.Write(&lp); err != nil || lp.GetHistogram().GetSampleCount() != 1 {
		t.Fatalf("long-poll histogram samples = %d (%v), want 1", lp.GetHistogram().GetSampleCount(), err)
	}
}

func TestRequestMetricsCountRecoveredPanics(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("bug") })
	h := Chain(mux, RequestID, Logging(discard()), Instrument(m), Recoverer(discard()))

	do(h, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if got := testutil.ToFloat64(m.requests.WithLabelValues("/boom", "GET", "500")); got != 1 {
		t.Fatalf("panicking request counted as %v 500s, want 1", got)
	}
}
