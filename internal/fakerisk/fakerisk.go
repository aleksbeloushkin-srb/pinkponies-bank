// Package fakerisk simulates an external screening provider: the service a
// bank asks "may this transfer go ahead?" before it moves money. It is not
// a microservice of the bank; it plays the third party in development, in
// tests and in the end-to-end script.
//
// POST /screenings takes an Idempotency-Key and answers approve, decline or
// review (202 with Retry-After: ask again later with the same key). It
// deduplicates by key like a real provider: a repeated request gets the
// same screening, whose decision may have moved from review to final.
//
// The decision is picked by the last two digits of the amount in minor
// units: ..66 is declined, ..77 is reviewed and then approved, ..78 is
// reviewed and then declined; anything else is approved.
//
// Failure injection, consumed one per call from a queue set through
// /_control or FAKERISK_FAULTS:
//
//	429        refused before executing, with Retry-After
//	500        failed before executing (the caller cannot know that)
//	hang       executed, then never answers until the caller gives up
//	ambiguous  executed, then the connection is dropped without an answer
//
// and a latency added to every answer after the screening is executed.
package fakerisk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Decisions.
const (
	Approve = "approve"
	Decline = "decline"
	Review  = "review"
)

// Faults.
const (
	Fault429       = "429"
	Fault500       = "500"
	FaultHang      = "hang"
	FaultAmbiguous = "ambiguous"
)

// KnownFaults lists the accepted fault names.
var KnownFaults = []string{Fault429, Fault500, FaultHang, FaultAmbiguous}

// Config tunes the simulator.
type Config struct {
	// Latency delays every answer, after the screening is executed.
	Latency time.Duration
	// RetryAfter is sent with injected 429 answers.
	RetryAfter time.Duration
	// ReviewDelay is how long a reviewed screening stays in review.
	ReviewDelay time.Duration
	// Faults is the initial fault queue.
	Faults []string
}

// Clock returns the current time; tests pin it.
type Clock interface {
	Now() time.Time
}

// Request is the body of POST /screenings.
type Request struct {
	TransferID           string `json:"transfer_id"`
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	AmountMinor          int64  `json:"amount_minor"`
	Currency             string `json:"currency"`
}

// Response is the answer of POST /screenings.
type Response struct {
	ID       string `json:"id"`
	Decision string `json:"decision"`
	Reason   string `json:"reason,omitempty"`
	// RetryAfterMS is set for a review: when to ask again.
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

type screening struct {
	id          string
	key         string
	fingerprint string
	final       string
	reason      string
	reviewUntil time.Time // zero without a review
	calls       int
}

// decision returns the decision visible at now.
func (s *screening) decision(now time.Time) string {
	if now.Before(s.reviewUntil) {
		return Review
	}
	return s.final
}

// Server is the simulator. Its zero value is not usable; call New.
type Server struct {
	cfg   Config
	clock Clock
	log   *slog.Logger
	// ctx ends when Close is called, releasing hanging requests.
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	byKey map[string]*screening
	// onHang, when set by a test, runs when a request starts hanging.
	onHang  func()
	faults  []string
	latency time.Duration
	stats   map[string]int
}

// New returns a Server.
func New(cfg Config, clock Clock, log *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{cfg: cfg, clock: clock, log: log, ctx: ctx, cancel: cancel, byKey: map[string]*screening{}}
	s.reset()
	return s
}

// Close releases every hanging request. Call it when the HTTP server shuts
// down, or Shutdown waits for them.
func (s *Server) Close() { s.cancel() }

func (s *Server) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = slices.Clone(s.cfg.Faults)
	s.latency = s.cfg.Latency
	s.stats = map[string]int{}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /screenings", s.screen)
	mux.HandleFunc("GET /_control", s.showControl)
	mux.HandleFunc("POST /_control", s.updateControl)
	mux.HandleFunc("DELETE /_control", s.resetControl)
	mux.HandleFunc("GET /_control/screening", s.showScreening)
	ok := func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
	mux.HandleFunc("GET /healthz", ok)
	mux.HandleFunc("GET /readyz", ok)
	return mux
}

func (s *Server) count(name string) {
	s.mu.Lock()
	s.stats[name]++
	s.mu.Unlock()
}

func (s *Server) nextFault() (fault string, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.faults) > 0 {
		fault, s.faults = s.faults[0], s.faults[1:]
	}
	return fault, s.latency
}

func (s *Server) screen(w http.ResponseWriter, r *http.Request) {
	s.count("calls")
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "error": "Idempotency-Key header is required"})
		return
	}
	var in Request
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&in); err != nil ||
		in.TransferID == "" || in.AmountMinor <= 0 || in.Currency == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request", "error": "invalid body"})
		return
	}
	// Drain the body: only then does net/http notice a caller that hangs up.
	_, _ = io.Copy(io.Discard, r.Body)

	fault, latency := s.nextFault()
	if fault != "" {
		s.log.InfoContext(r.Context(), "fault injected", "fault", fault)
	}
	switch fault {
	case Fault429:
		s.count("fault_429")
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(s.cfg.RetryAfter.Seconds()))))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"code": "rate_limited", "error": "too many requests"})
		return
	case Fault500:
		s.count("fault_500")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "error": "internal error"})
		return
	}

	sc, created, err := s.store(key, in)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"code": "idempotency_key_reused", "error": err.Error()})
		return
	}
	switch fault {
	case FaultHang:
		// Executed, but the caller never hears about it: the ambiguous
		// outcome a client has to survive by asking again with the key.
		s.mu.Lock()
		s.stats["fault_hang"]++
		onHang := s.onHang
		s.mu.Unlock()
		if onHang != nil {
			onHang()
		}
		select {
		case <-r.Context().Done():
		case <-s.ctx.Done():
		}
		return
	case FaultAmbiguous:
		s.count("fault_ambiguous")
		// net/http closes the connection without writing a response.
		panic(http.ErrAbortHandler)
	}
	if !sleep(r.Context(), latency) {
		return
	}

	now := s.clock.Now()
	s.mu.Lock()
	resp := Response{ID: sc.id, Decision: sc.decision(now)}
	if resp.Decision != Review {
		resp.Reason = sc.reason
	}
	reviewLeft := sc.reviewUntil.Sub(now)
	s.stats["decisions_"+resp.Decision]++
	s.mu.Unlock()

	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	if resp.Decision == Review {
		wait := max(reviewLeft, time.Second)
		resp.RetryAfterMS = wait.Milliseconds()
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
		writeJSON(w, http.StatusAccepted, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// store deduplicates by Idempotency-Key, as real providers do. The same key
// with another body is refused.
func (s *Server) store(key string, in Request) (*screening, bool, error) {
	fp := fmt.Sprintf("%s|%s|%s|%d|%s", in.TransferID, in.SourceAccountID, in.DestinationAccountID, in.AmountMinor, in.Currency)
	s.mu.Lock()
	defer s.mu.Unlock()
	if sc, ok := s.byKey[key]; ok {
		if sc.fingerprint != fp {
			return nil, false, fmt.Errorf("idempotency key %q reused with a different request", key)
		}
		sc.calls++
		s.stats["replays"]++
		return sc, false, nil
	}
	final, reason, review := rules(in.AmountMinor)
	sc := &screening{id: "scr_" + randomHex(8), key: key, fingerprint: fp, final: final, reason: reason, calls: 1}
	if review {
		sc.reviewUntil = s.clock.Now().Add(s.cfg.ReviewDelay)
	}
	s.byKey[key] = sc
	s.stats["executions"]++
	return sc, true, nil
}

// rules picks the outcome from the last two digits of the amount.
func rules(amountMinor int64) (final, reason string, review bool) {
	switch amountMinor % 100 {
	case 66:
		return Decline, "declined by the demo rules (amount ending in 66)", false
	case 77:
		return Approve, "approved after review", true
	case 78:
		return Decline, "declined after review", true
	default:
		return Approve, "", false
	}
}

type controlView struct {
	Faults  []string       `json:"faults"`
	Latency string         `json:"latency"`
	Stats   map[string]int `json:"stats"`
}

func (s *Server) control() controlView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return controlView{Faults: slices.Clone(s.faults), Latency: s.latency.String(), Stats: maps.Clone(s.stats)}
}

func (s *Server) showControl(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.control())
}

// updateControl changes fault injection at run time:
//
//	POST /_control?faults=429,hang   append to the fault queue
//	POST /_control?latency=300ms     delay of each answer, after executing
func (s *Server) updateControl(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	faults, err := ParseFaults(q.Get("faults"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var latency time.Duration
	if v := q.Get("latency"); v != "" {
		if latency, err = time.ParseDuration(v); err != nil || latency < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid latency"})
			return
		}
	}
	s.mu.Lock()
	s.faults = append(s.faults, faults...)
	if q.Has("latency") {
		s.latency = latency
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, s.control())
}

func (s *Server) resetControl(w http.ResponseWriter, _ *http.Request) {
	s.reset()
	writeJSON(w, http.StatusOK, s.control())
}

// showScreening reports one screening by key: how many calls it took and
// its decision. The end-to-end script uses it to check "executed once".
func (s *Server) showScreening(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sc, ok := s.byKey[r.URL.Query().Get("key")]
	var out map[string]any
	if ok {
		out = map[string]any{"id": sc.id, "calls": sc.calls, "decision": sc.decision(s.clock.Now())}
	}
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown key"})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ParseFaults reads a comma-separated fault list such as "429,hang".
func ParseFaults(s string) ([]string, error) {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		if !slices.Contains(KnownFaults, f) {
			return nil, fmt.Errorf("unknown fault %q (want one of %v)", f, KnownFaults)
		}
		out = append(out, f)
	}
	return out, nil
}

// sleep pauses for d unless ctx ends first; it reports whether the full
// pause elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
