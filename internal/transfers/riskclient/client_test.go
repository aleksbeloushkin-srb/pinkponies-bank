package riskclient

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/fakerisk"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// against runs a client with the given call timeout against the real fake
// provider with faults. patient is a second client with a long timeout,
// for calls that must not time out on a slow machine.
func against(t *testing.T, cfg fakerisk.Config, timeout time.Duration) (c *Client, m *Metrics, patient *Client) {
	t.Helper()
	risk := fakerisk.New(cfg, fixedClock{time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(risk.Handler())
	t.Cleanup(func() {
		risk.Close()
		srv.Close()
	})
	m = NewMetrics(prometheus.NewRegistry())
	c, err := New(Config{BaseURL: srv.URL, CallTimeout: timeout, MaxRetryAfter: time.Minute}, srv.Client(), m)
	if err != nil {
		t.Fatal(err)
	}
	patient, err = New(Config{BaseURL: srv.URL, CallTimeout: 10 * time.Second, MaxRetryAfter: time.Minute}, srv.Client(), NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	return c, m, patient
}

func request(amount int64) app.ScreeningRequest {
	id := uuid.New()
	return app.ScreeningRequest{Key: "transfer:" + id.String() + ":screen", TransferID: id, SourceID: uuid.New(),
		DestinationID: uuid.New(), AmountMinor: amount, Currency: "EUR"}
}

func TestDecisions(t *testing.T) {
	c, m, _ := against(t, fakerisk.Config{ReviewDelay: 5 * time.Second}, 10*time.Second)
	for amount, want := range map[int64]app.Decision{1250: app.DecisionApprove, 1266: app.DecisionDecline, 1277: app.DecisionReview} {
		res, err := c.Screen(context.Background(), request(amount))
		if err != nil || res.Decision != want || res.ID == "" {
			t.Fatalf("amount %d: %+v, %v", amount, res, err)
		}
		if want == app.DecisionReview && res.RetryAfter != 5*time.Second {
			t.Fatalf("review retry after %s, want 5s", res.RetryAfter)
		}
	}
	for _, o := range []string{outcomeApprove, outcomeDecline, outcomeReview} {
		if got := testutil.ToFloat64(m.requests.WithLabelValues(o)); got != 1 {
			t.Fatalf("outcome %s counted %v", o, got)
		}
	}
}

// The same key after a lost answer gets the same screening: retrying is
// what makes ambiguous answers harmless.
func TestFaultsAreClassified(t *testing.T) {
	tests := []struct {
		fault   string
		outcome string
	}{
		{fakerisk.Fault429, outcomeRateLimited},
		{fakerisk.Fault500, outcomeAmbiguous},
		{fakerisk.FaultHang, outcomeAmbiguous},
		{fakerisk.FaultAmbiguous, outcomeAmbiguous},
	}
	for _, tt := range tests {
		t.Run(tt.fault, func(t *testing.T) {
			c, m, patient := against(t, fakerisk.Config{Faults: []string{tt.fault}, RetryAfter: 3 * time.Second}, 100*time.Millisecond)
			req := request(1250)
			_, err := c.Screen(context.Background(), req)
			if !errors.Is(err, app.ErrScreeningUnavailable) {
				t.Fatalf("err = %v, want ErrScreeningUnavailable", err)
			}
			if got := testutil.ToFloat64(m.requests.WithLabelValues(tt.outcome)); got != 1 {
				t.Fatalf("outcome %s counted %v", tt.outcome, got)
			}
			if tt.fault == fakerisk.Fault429 {
				var hint *app.RetryAfterError
				if !errors.As(err, &hint) || hint.RetryAfter() != 3*time.Second {
					t.Fatalf("429 must carry Retry-After: %v", err)
				}
			}
			res, err := patient.Screen(context.Background(), req)
			if err != nil || res.Decision != app.DecisionApprove {
				t.Fatalf("retry with the same key = %+v, %v", res, err)
			}
		})
	}
}

func TestStatusCodes(t *testing.T) {
	var (
		mu   sync.Mutex
		code int
		key  string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key = r.Header.Get("Idempotency-Key")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"code":"x"}`))
	}))
	defer srv.Close()
	m := NewMetrics(prometheus.NewRegistry())
	c, err := New(Config{BaseURL: srv.URL, CallTimeout: time.Second, MaxRetryAfter: time.Minute}, srv.Client(), m)
	if err != nil {
		t.Fatal(err)
	}
	for status, outcome := range map[int]string{
		http.StatusConflict: outcomeAmbiguous, http.StatusTooEarly: outcomeAmbiguous, http.StatusBadGateway: outcomeAmbiguous,
		http.StatusOK:         outcomeAmbiguous, // unreadable 2xx
		http.StatusBadRequest: outcomeMisconfigured, http.StatusUnauthorized: outcomeMisconfigured,
		http.StatusUnprocessableEntity: outcomeMisconfigured,
	} {
		mu.Lock()
		code = status
		mu.Unlock()
		before := testutil.ToFloat64(m.requests.WithLabelValues(outcome))
		req := request(100)
		if _, err := c.Screen(context.Background(), req); !errors.Is(err, app.ErrScreeningUnavailable) {
			t.Errorf("%d: err = %v", status, err)
		}
		if got := testutil.ToFloat64(m.requests.WithLabelValues(outcome)); got != before+1 {
			t.Errorf("%d: outcome %s not counted", status, outcome)
		}
		mu.Lock()
		if key != req.Key {
			t.Errorf("%d: Idempotency-Key = %q", status, key)
		}
		mu.Unlock()
	}
}

// A connection refused before anything was written is "not sent", the
// only failure known not to have executed.
func TestDialErrorIsUnavailable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	m := NewMetrics(prometheus.NewRegistry())
	c, err := New(Config{BaseURL: "http://" + addr, CallTimeout: time.Second, MaxRetryAfter: time.Minute}, &http.Client{}, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Screen(context.Background(), request(100)); !errors.Is(err, app.ErrScreeningUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues(outcomeUnavailable)); got != 1 {
		t.Fatalf("unavailable counted %v", got)
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "localhost:8090", "ftp://x"} {
		if _, err := New(Config{BaseURL: u, CallTimeout: time.Second, MaxRetryAfter: time.Minute}, http.DefaultClient, NewMetrics(prometheus.NewRegistry())); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
}

// Retry-After comes from outside: a long wait, in seconds or as a date
// years ahead, is cut to MaxRetryAfter, so the provider cannot park a saga
// step that holds money.
func TestRetryAfterIsCapped(t *testing.T) {
	for _, header := range []string{"31536000", time.Now().AddDate(300, 0, 0).UTC().Format(http.TimeFormat), "7"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", header)
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		c, err := New(Config{BaseURL: srv.URL, CallTimeout: 5 * time.Second, MaxRetryAfter: 20 * time.Second}, srv.Client(), NewMetrics(prometheus.NewRegistry()))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Screen(context.Background(), request(100))
		srv.Close()
		var hint *app.RetryAfterError
		if !errors.As(err, &hint) {
			t.Fatalf("Retry-After %q: err = %v", header, err)
		}
		want := 20 * time.Second
		if header == "7" {
			want = 7 * time.Second
		}
		if hint.RetryAfter() != want {
			t.Fatalf("Retry-After %q: wait %s, want %s", header, hint.RetryAfter(), want)
		}
	}
}

func TestNewRejectsMissingTimeouts(t *testing.T) {
	for _, cfg := range []Config{{BaseURL: "http://x", MaxRetryAfter: time.Minute}, {BaseURL: "http://x", CallTimeout: time.Second}} {
		if _, err := New(cfg, http.DefaultClient, NewMetrics(prometheus.NewRegistry())); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}
