package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
)

func sample() notifications.Notification {
	return notifications.Notification{ID: uuid.New(), AccountID: uuid.New(), TransferID: uuid.New(),
		Kind: notifications.KindSent, Message: "You sent 12.50 EUR", CreatedAt: time.Unix(10, 0)}
}

func TestWebhookSendsWithIdempotencyKey(t *testing.T) {
	n := sample()
	var status atomic.Int32
	status.Store(http.StatusAccepted)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body webhookBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID != n.ID || body.Kind != "transfer_sent" {
			t.Errorf("body %+v, %v", body, err)
		}
		if r.Header.Get("Idempotency-Key") != n.ID.String() {
			t.Errorf("Idempotency-Key = %q", r.Header.Get("Idempotency-Key"))
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	wh, err := NewWebhook(srv.URL, srv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := wh.Send(context.Background(), n); err != nil {
		t.Fatalf("Send = %v", err)
	}
	status.Store(http.StatusInternalServerError)
	if err := wh.Send(context.Background(), n); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("Send on 500 = %v", err)
	}
}

func TestWebhookTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	wh, _ := NewWebhook(srv.URL, srv.Client(), 50*time.Millisecond)
	if err := wh.Send(context.Background(), sample()); err == nil {
		t.Fatal("a hanging receiver must fail the send")
	}
}

func TestLogChannelLogs(t *testing.T) {
	var buf bytes.Buffer
	if err := NewLog(slog.New(slog.NewJSONHandler(&buf, nil))).Send(context.Background(), sample()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"kind":"transfer_sent"`) {
		t.Fatalf("log = %s", buf.String())
	}
}

func TestNewWebhookRejectsBadURL(t *testing.T) {
	if _, err := NewWebhook("sink:8081", http.DefaultClient, time.Second); err == nil {
		t.Fatal("want an error")
	}
}
