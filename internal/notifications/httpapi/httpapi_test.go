package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
)

type fakeLister struct {
	got   uuid.UUID
	limit int
	list  []notifications.Notification
	err   error
}

func (f *fakeLister) Notifications(_ context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error) {
	f.got, f.limit = account, limit
	return f.list, f.err
}

func newHandler(l Lister, sink *Sink) http.Handler {
	return NewHandler(Deps{
		Lister: l, CORSOrigins: []string{"http://localhost:8080"}, Sink: sink,
		Metrics: httpserver.NewMetrics(prometheus.NewRegistry()),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func do(h http.Handler, method, path string, header ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestListNotifications(t *testing.T) {
	account := uuid.New()
	l := &fakeLister{list: []notifications.Notification{{
		ID: uuid.New(), AccountID: account, Kind: notifications.KindReceived, Message: "You received 1.00 EUR",
		Deliveries: []notifications.Delivery{{Channel: "webhook", State: notifications.DeliveryDelivered, Attempts: 2, DeliveredAt: time.Unix(5, 0)}},
	}}}
	rec := do(newHandler(l, nil), http.MethodGet, "/v1/notifications?account="+account.String()+"&limit=10")
	var body struct {
		Notifications []notificationJSON `json:"notifications"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if l.got != account || l.limit != 10 || len(body.Notifications) != 1 || body.Notifications[0].Deliveries[0].Attempts != 2 {
		t.Fatalf("body %+v", body)
	}
	for _, bad := range []string{"/v1/notifications", "/v1/notifications?account=pony", "/v1/notifications?account=" + account.String() + "&limit=-1"} {
		if rec := do(newHandler(l, nil), http.MethodGet, bad); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", bad, rec.Code)
		}
	}
	l.err = errors.New("database down")
	if rec := do(newHandler(l, nil), http.MethodGet, "/v1/notifications?account="+account.String()); rec.Code != 500 || strings.Contains(rec.Body.String(), "database") {
		t.Fatalf("error = %d %s", rec.Code, rec.Body)
	}
}

func TestCORS(t *testing.T) {
	h := newHandler(&fakeLister{}, nil)
	path := "/v1/notifications?account=" + uuid.NewString()
	rec := do(h, http.MethodGet, path, "Origin", "http://localhost:8080")
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:8080" || rec.Header().Get("Vary") != "Origin" {
		t.Fatalf("allowed origin headers = %v", rec.Header())
	}
	rec = do(h, http.MethodGet, path, "Origin", "http://evil.example")
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("an unlisted origin must get no CORS grant")
	}
	rec = do(h, http.MethodOptions, path, "Origin", "http://localhost:8080", "Access-Control-Request-Method", "GET")
	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Methods") != "GET" {
		t.Fatalf("preflight = %d %v", rec.Code, rec.Header())
	}
}

func TestSink(t *testing.T) {
	sink, err := NewSink([]string{SinkFault500})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	h := newHandler(&fakeLister{}, sink)
	if rec := do(h, http.MethodPost, "/_sink/webhooks", "Idempotency-Key", "n1"); rec.Code != 500 {
		t.Fatalf("injected fault = %d", rec.Code)
	}
	for range 2 {
		if rec := do(h, http.MethodPost, "/_sink/webhooks", "Idempotency-Key", "n1"); rec.Code != http.StatusNoContent {
			t.Fatalf("accept = %d", rec.Code)
		}
	}
	if st := sink.Stats(); st.Calls != 3 || st.Accepted != 2 || st.Unique != 1 || st.Duplicates != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if rec := do(h, http.MethodPost, "/_sink/control?faults=boom"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad fault = %d", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/_sink/control"); rec.Code != 200 || sink.Stats().Calls != 0 {
		t.Fatal("reset")
	}
	if _, err := NewSink([]string{"boom"}); err == nil {
		t.Fatal("unknown fault accepted")
	}
}

func TestSinkHangIsReleasedByClose(t *testing.T) {
	sink, _ := NewSink([]string{SinkFaultHang})
	h := newHandler(&fakeLister{}, sink)
	done := make(chan int)
	go func() { done <- do(h, http.MethodPost, "/_sink/webhooks", "Idempotency-Key", "n1").Code }()
	sink.Close()
	<-done
	if st := sink.Stats(); st.Accepted != 0 {
		t.Fatalf("a hung call must not count as accepted: %+v", st)
	}
}
