// Package riskclient is the transfers service's HTTP client of the
// screening provider: it implements app.Screener.
//
// Every call carries the step's idempotency key and its own deadline. The
// provider deduplicates by key, so the saga may repeat a call whose outcome
// it does not know. The client itself never retries: the task queue does,
// with backoff, and the saga bounds the number of failures.
//
// What the client reports, and the outcome label of
// screening_requests_total:
//
//	200 approve or decline          a decision            approve, decline
//	202 review                      "ask again later"     review
//	429                             not executed; retry   rate_limited
//	                                after Retry-After
//	dial error, nothing written     not executed          unavailable
//	timeout, reset, 5xx, 408, 409,  may have executed     ambiguous
//	425, unreadable 2xx
//	other 4xx                       our request or        misconfigured
//	                                credentials are wrong
//
// Every failure wraps app.ErrScreeningUnavailable: whatever the cause, the
// saga's answer is the same, a retry with the same key. The distinction is
// kept for the metrics, the alerts and the logs.
package riskclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

// Outcomes, the outcome label of the metrics.
const (
	outcomeApprove       = "approve"
	outcomeDecline       = "decline"
	outcomeReview        = "review"
	outcomeRateLimited   = "rate_limited"
	outcomeUnavailable   = "unavailable"
	outcomeAmbiguous     = "ambiguous"
	outcomeMisconfigured = "misconfigured"
)

var outcomes = []string{outcomeApprove, outcomeDecline, outcomeReview, outcomeRateLimited, outcomeUnavailable, outcomeAmbiguous, outcomeMisconfigured}

const (
	maxResponseBytes  = 64 << 10
	defaultRetryAfter = time.Second
)

// Metrics are the client's Prometheus metrics, one observation per call.
type Metrics struct {
	requests *prometheus.CounterVec
	duration prometheus.Histogram
}

// NewMetrics creates the metrics and registers them on reg, every outcome
// at zero so that the ambiguous-ratio alert sees the first one.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "screening_requests_total",
			Help: "Calls to the screening provider by classified outcome: approve, decline, review, rate_limited, unavailable, ambiguous, misconfigured.",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "screening_request_duration_seconds",
			Help:    "Duration of one call to the screening provider.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4},
		}),
	}
	reg.MustRegister(m.requests, m.duration)
	for _, o := range outcomes {
		m.requests.WithLabelValues(o)
	}
	return m
}

// Config tunes the client.
type Config struct {
	BaseURL string
	// CallTimeout bounds one call; the effective deadline is the earlier of
	// this and the caller's.
	CallTimeout time.Duration
}

// Client calls the screening provider.
type Client struct {
	endpoint string
	http     *http.Client
	metrics  *Metrics
	cfg      Config
}

// New returns a Client for cfg.BaseURL using hc.
func New(cfg Config, hc *http.Client, m *Metrics) (*Client, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("riskclient: invalid base URL %q", cfg.BaseURL)
	}
	return &Client{endpoint: base.JoinPath("screenings").String(), http: hc, metrics: m, cfg: cfg}, nil
}

type screeningRequest struct {
	TransferID           string `json:"transfer_id"`
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	AmountMinor          int64  `json:"amount_minor"`
	Currency             string `json:"currency"`
}

type screeningResponse struct {
	ID           string `json:"id"`
	Decision     string `json:"decision"`
	Reason       string `json:"reason"`
	RetryAfterMS int64  `json:"retry_after_ms"`
}

// Screen implements app.Screener.
func (c *Client) Screen(ctx context.Context, req app.ScreeningRequest) (app.Screening, error) {
	start := time.Now()
	res, outcome, err := c.screen(ctx, req)
	c.metrics.duration.Observe(time.Since(start).Seconds())
	c.metrics.requests.WithLabelValues(outcome).Inc()
	return res, err
}

func (c *Client) screen(ctx context.Context, req app.ScreeningRequest) (app.Screening, string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	body, err := json.Marshal(screeningRequest{
		TransferID:           req.TransferID.String(),
		SourceAccountID:      req.SourceID.String(),
		DestinationAccountID: req.DestinationID.String(),
		AmountMinor:          req.AmountMinor,
		Currency:             req.Currency,
	})
	if err != nil {
		return app.Screening{}, outcomeMisconfigured, unavailable("encode request", err)
	}
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		// Called after every write attempt, also a failed one: the bytes may
		// have reached the provider, so any call counts as written.
		WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) },
	})
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return app.Screening{}, outcomeMisconfigured, unavailable("build request", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")
	hreq.Header.Set("Idempotency-Key", req.Key)

	resp, err := c.http.Do(hreq)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" && !wrote.Load() {
			return app.Screening{}, outcomeUnavailable, unavailable("not sent", err)
		}
		return app.Screening{}, outcomeAmbiguous, unavailable("outcome unknown", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()
	return classify(resp)
}

func classify(resp *http.Response) (app.Screening, string, error) {
	code := resp.StatusCode
	switch {
	case code == http.StatusOK || code == http.StatusAccepted:
		var body screeningResponse
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&body); err != nil || body.ID == "" {
			return app.Screening{}, outcomeAmbiguous, unavailable("unreadable answer", err)
		}
		switch {
		case code == http.StatusOK && body.Decision == string(app.DecisionApprove):
			return app.Screening{ID: body.ID, Decision: app.DecisionApprove, Reason: body.Reason}, outcomeApprove, nil
		case code == http.StatusOK && body.Decision == string(app.DecisionDecline):
			return app.Screening{ID: body.ID, Decision: app.DecisionDecline, Reason: body.Reason}, outcomeDecline, nil
		case code == http.StatusAccepted && body.Decision == string(app.DecisionReview):
			after := time.Duration(body.RetryAfterMS) * time.Millisecond
			if after <= 0 {
				after = retryAfter(resp.Header.Get("Retry-After"))
			}
			return app.Screening{ID: body.ID, Decision: app.DecisionReview, RetryAfter: after}, outcomeReview, nil
		}
		return app.Screening{}, outcomeAmbiguous, unavailable(fmt.Sprintf("status %d with decision %q", code, body.Decision), nil)
	case code == http.StatusTooManyRequests:
		after := retryAfter(resp.Header.Get("Retry-After"))
		return app.Screening{}, outcomeRateLimited, &app.RetryAfterError{
			After: after,
			Err:   unavailable(fmt.Sprintf("rate limited, retry after %s", after), nil),
		}
	case code == http.StatusRequestTimeout, code == http.StatusConflict, code == http.StatusTooEarly, code >= 500:
		// 409 is "a request with this key is in progress", 425 "too early":
		// both expect the same request again.
		return app.Screening{}, outcomeAmbiguous, unavailable(fmt.Sprintf("status %d", code), nil)
	default:
		return app.Screening{}, outcomeMisconfigured, unavailable(fmt.Sprintf("status %d: the provider refuses our request or credentials", code), nil)
	}
}

func unavailable(what string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: %s", app.ErrScreeningUnavailable, what)
	}
	return fmt.Errorf("%w: %s: %w", app.ErrScreeningUnavailable, what, err)
}

// retryAfter parses a Retry-After header given in seconds or as an HTTP
// date.
func retryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0)
	}
	return defaultRetryAfter
}
