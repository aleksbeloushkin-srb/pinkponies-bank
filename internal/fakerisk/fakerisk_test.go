package fakerisk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fixture struct {
	srv    *Server
	http   *httptest.Server
	clock  *clock
	client *http.Client
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{clock: &clock{now: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)}}
	f.srv = New(cfg, f.clock, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.http = httptest.NewServer(f.srv.Handler())
	f.client = f.http.Client()
	t.Cleanup(func() {
		f.srv.Close()
		f.http.Close()
	})
	return f
}

func (f *fixture) post(t *testing.T, key string, amount int64) (*http.Response, Response, error) {
	t.Helper()
	body, _ := json.Marshal(Request{TransferID: "t-" + key, SourceAccountID: "a", DestinationAccountID: "b", AmountMinor: amount, Currency: "EUR"})
	req, _ := http.NewRequest(http.MethodPost, f.http.URL+"/screenings", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, Response{}, err
	}
	defer resp.Body.Close()
	var out Response
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out, nil
}

func (f *fixture) stat(name string) int { return f.srv.control().Stats[name] }

func TestDecisionsByAmount(t *testing.T) {
	f := newFixture(t, Config{ReviewDelay: time.Minute})
	for amount, want := range map[int64]string{1250: Approve, 1266: Decline, 1277: Review, 1278: Review} {
		resp, out, err := f.post(t, fmt.Sprint("transfer:", amount, ":screen"), amount)
		if err != nil {
			t.Fatal(err)
		}
		if out.Decision != want {
			t.Errorf("amount %d: %s, want %s", amount, out.Decision, want)
		}
		if want == Review && (resp.StatusCode != http.StatusAccepted || resp.Header.Get("Retry-After") != "60" || out.RetryAfterMS != 60_000) {
			t.Errorf("amount %d: review answered %d, Retry-After %q, %d ms", amount, resp.StatusCode, resp.Header.Get("Retry-After"), out.RetryAfterMS)
		}
		if want != Review && resp.StatusCode != http.StatusOK {
			t.Errorf("amount %d: status %d", amount, resp.StatusCode)
		}
	}
}

// A replay gets the same screening; a reviewed one becomes final once the
// review delay has passed.
func TestReplayAndReviewResolution(t *testing.T) {
	f := newFixture(t, Config{ReviewDelay: 10 * time.Second})
	_, first, _ := f.post(t, "transfer:1:screen", 4278)
	if first.Decision != Review {
		t.Fatalf("first = %+v", first)
	}
	f.clock.Advance(10 * time.Second)
	resp, again, _ := f.post(t, "transfer:1:screen", 4278)
	if again.ID != first.ID || again.Decision != Decline || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("after review: %+v, replayed header %q", again, resp.Header.Get("Idempotent-Replayed"))
	}
	if f.stat("executions") != 1 || f.stat("replays") != 1 {
		t.Fatalf("stats = %v", f.srv.control().Stats)
	}
	resp, _, _ = f.post(t, "transfer:1:screen", 4279)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("same key, other amount: %d, want 422", resp.StatusCode)
	}
}

// 429 and 500 are answered before executing: no screening is stored.
func TestFaultsBeforeExecution(t *testing.T) {
	f := newFixture(t, Config{RetryAfter: 2 * time.Second, Faults: []string{Fault429, Fault500}})
	resp, _, _ := f.post(t, "k", 100)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "2" {
		t.Fatalf("first: %d Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	resp, _, _ = f.post(t, "k", 100)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("second: %d", resp.StatusCode)
	}
	if f.stat("executions") != 0 {
		t.Fatal("refused requests must not execute")
	}
	resp, out, _ := f.post(t, "k", 100)
	if resp.StatusCode != http.StatusOK || out.Decision != Approve || f.stat("executions") != 1 {
		t.Fatalf("third: %d %+v", resp.StatusCode, out)
	}
}

// hang executes and then answers nothing until the caller gives up; the
// retry with the same key finds the screening. The caller gives up when the
// server reports the request parked, so no timer decides the outcome.
func TestHangExecutesThenRetryReplays(t *testing.T) {
	f := newFixture(t, Config{Faults: []string{FaultHang}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.srv.mu.Lock()
	f.srv.onHang = cancel
	f.srv.mu.Unlock()
	body, _ := json.Marshal(Request{TransferID: "t", AmountMinor: 100, Currency: "EUR"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.http.URL+"/screenings", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", "k")
	if _, err := f.client.Do(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the caller giving up", err)
	}
	if f.stat("executions") != 1 || f.stat("fault_hang") != 1 {
		t.Fatalf("stats = %v", f.srv.control().Stats)
	}
	body, _ = json.Marshal(Request{TransferID: "t", AmountMinor: 100, Currency: "EUR"})
	req, _ = http.NewRequest(http.MethodPost, f.http.URL+"/screenings", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", "k")
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry = %d", resp.StatusCode)
	}
}

func TestAmbiguousExecutesAndDropsTheConnection(t *testing.T) {
	f := newFixture(t, Config{Faults: []string{FaultAmbiguous}})
	if _, _, err := f.post(t, "k", 100); err == nil {
		t.Fatal("an aborted answer must be a transport error")
	}
	if f.stat("executions") != 1 {
		t.Fatalf("stats = %v", f.srv.control().Stats)
	}
}

func TestCloseReleasesHangingRequests(t *testing.T) {
	f := newFixture(t, Config{Faults: []string{FaultHang}})
	parked := make(chan struct{})
	f.srv.mu.Lock()
	f.srv.onHang = func() { close(parked) }
	f.srv.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, _, err := f.post(t, "k", 100)
		done <- err
	}()
	<-parked
	f.srv.Close()
	if err := <-done; err != nil {
		t.Fatalf("a released hang must end the request: %v", err)
	}
}

func TestControl(t *testing.T) {
	f := newFixture(t, Config{})
	resp, err := f.client.Post(f.http.URL+"/_control?faults=429,hang&latency=5ms", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if c := f.srv.control(); len(c.Faults) != 2 || c.Latency != "5ms" {
		t.Fatalf("control = %+v", c)
	}
	resp, _ = f.client.Post(f.http.URL+"/_control?faults=boom", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown fault accepted: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodDelete, f.http.URL+"/_control", nil)
	if resp, err = f.client.Do(req); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if c := f.srv.control(); len(c.Faults) != 0 || c.Latency != "0s" {
		t.Fatalf("after reset: %+v", c)
	}

	f.post(t, "transfer:9:screen", 100)
	f.post(t, "transfer:9:screen", 100)
	resp, err = f.client.Get(f.http.URL + "/_control/screening?key=transfer:9:screen")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sc struct {
		Calls    int    `json:"calls"`
		Decision string `json:"decision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sc); err != nil || sc.Calls != 2 || sc.Decision != Approve {
		t.Fatalf("screening view = %+v, %v", sc, err)
	}
}
