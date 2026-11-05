package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/longpoll"
)

var sample = transfers.Transfer{
	ID: uuid.New(), SourceAccountID: uuid.New(), DestinationAccountID: uuid.New(),
	AmountMinor: 1250, Currency: "EUR", Status: transfers.StatusHeld, Version: 3, HoldID: uuid.New(),
	Steps: []transfers.StepRecord{{Step: transfers.StepHold, State: transfers.StepSucceeded, Outcome: "placed", Attempts: 1}},
}

type fakeService struct {
	createRes app.CreateTransferResult
	createErr error
	got       app.CreateTransferRequest
	readErr   error
	waitRes   app.WaitResult
	waitErr   error
	accounts  []app.Account
	listErr   error
}

func (f *fakeService) CreateTransfer(_ context.Context, req app.CreateTransferRequest) (app.CreateTransferResult, error) {
	f.got = req
	return f.createRes, f.createErr
}

func (f *fakeService) Transfer(context.Context, uuid.UUID) (transfers.Transfer, error) {
	return sample, f.readErr
}

func (f *fakeService) WaitTransfer(context.Context, uuid.UUID, int64) (app.WaitResult, error) {
	return f.waitRes, f.waitErr
}

func (f *fakeService) Accounts(context.Context) ([]app.Account, error) {
	return f.accounts, f.listErr
}

func newHandler(svc *fakeService) http.Handler {
	return NewHandler(Deps{
		Creator: svc, Reader: svc, Accounts: svc,
		Static:           fstest.MapFS{"index.html": {Data: []byte("<h1>Pinkponies Bank</h1>")}},
		NotificationsURL: "http://localhost:8081",
		Metrics:          httpserver.NewMetrics(prometheus.NewRegistry(), WaitPattern),
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func do(h http.Handler, method, path, body string, header ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q: %v", rec.Body, err)
	}
	return m
}

func createBody() string {
	return fmt.Sprintf(`{"source_account_id":%q,"destination_account_id":%q,"amount_minor":1250,"currency":"EUR"}`,
		sample.SourceAccountID, sample.DestinationAccountID)
}

func TestCreateTransfer(t *testing.T) {
	svc := &fakeService{createRes: app.CreateTransferResult{Transfer: sample, Created: true}}
	h := newHandler(svc)

	rec := do(h, http.MethodPost, "/v1/transfers", createBody(), "Idempotency-Key", "k-1")
	if rec.Code != http.StatusCreated || rec.Header().Get("Location") != "/v1/transfers/"+sample.ID.String() {
		t.Fatalf("create = %d %s", rec.Code, rec.Body)
	}
	if svc.got.IdempotencyKey != "k-1" || svc.got.AmountMinor != 1250 || svc.got.SourceAccountID != sample.SourceAccountID {
		t.Fatalf("request = %+v", svc.got)
	}
	body := decode(t, rec)
	if body["status"] != "held" || body["version"] != float64(3) || len(body["steps"].([]any)) != 1 {
		t.Fatalf("body = %v", body)
	}

	svc.createRes.Created = false
	if rec := do(h, http.MethodPost, "/v1/transfers", createBody(), "Idempotency-Key", "k-1"); rec.Code != http.StatusOK {
		t.Fatalf("replay = %d", rec.Code)
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err  error
		code int
		body string
	}{
		{&transfers.ValidationError{Field: "amount_minor", Reason: "must be positive"}, 400, "invalid_argument"},
		{transfers.ErrIdempotencyConflict, 422, "idempotency_conflict"},
		{transfers.ErrNotFound, 404, "not_found"},
		{fmt.Errorf("wrapped: %w", app.ErrLedgerUnavailable), 503, "upstream_unavailable"},
		{errors.New("disk on fire"), 500, "internal"},
	}
	for _, tt := range tests {
		h := newHandler(&fakeService{createErr: tt.err})
		rec := do(h, http.MethodPost, "/v1/transfers", createBody(), "Idempotency-Key", "k")
		if rec.Code != tt.code || decode(t, rec)["code"] != tt.body {
			t.Errorf("%v: %d %s", tt.err, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), "disk") {
			t.Error("an internal error leaked to the client")
		}
	}
}

func TestBadBodies(t *testing.T) {
	h := newHandler(&fakeService{})
	for name, body := range map[string]string{
		"not JSON":      "amount=5",
		"unknown field": `{"amount_minor":1,"amonut":2}`,
		"bad uuid":      `{"source_account_id":"pony"}`,
	} {
		if rec := do(h, http.MethodPost, "/v1/transfers", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	if rec := do(h, http.MethodPost, "/v1/transfers", `{"currency":"`+strings.Repeat("E", maxCreateBody)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("large body: %d", rec.Code)
	}
}

func TestWaitTransfer(t *testing.T) {
	svc := &fakeService{waitRes: app.WaitResult{Transfer: sample, Modified: true}}
	h := newHandler(svc)
	rec := do(h, http.MethodGet, "/v1/transfers/"+sample.ID.String()+"/wait?version=2", "")
	if rec.Code != http.StatusOK || decode(t, rec)["modified"] != true {
		t.Fatalf("wait = %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, http.MethodGet, "/v1/transfers/"+sample.ID.String()+"/wait?version=x", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad version = %d", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/v1/transfers/not-a-uuid/wait?version=1", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id = %d", rec.Code)
	}
	svc.waitErr = longpoll.ErrTooManyWaiters
	if rec := do(h, http.MethodGet, "/v1/transfers/"+sample.ID.String()+"/wait?version=1", ""); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("full registry = %d", rec.Code)
	}
	svc.waitErr = longpoll.ErrClosed
	if rec := do(h, http.MethodGet, "/v1/transfers/"+sample.ID.String()+"/wait?version=1", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed registry = %d", rec.Code)
	}
}

// A client that hung up is logged as 499, not as a success or a 500.
func TestClientGoneIs499(t *testing.T) {
	h := newHandler(&fakeService{waitErr: context.Canceled})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/transfers/"+sample.ID.String()+"/wait?version=1", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != httpserver.StatusClientClosedRequest {
		t.Fatalf("code = %d, want 499", rec.Code)
	}
}

func TestAccountsAndUI(t *testing.T) {
	svc := &fakeService{accounts: []app.Account{{ID: uuid.New(), Name: "Clover", Currency: "EUR", PostedMinor: 10, AvailableMinor: 7, HeldMinor: 3}}}
	h := newHandler(svc)
	rec := do(h, http.MethodGet, "/v1/accounts", "")
	accounts := decode(t, rec)["accounts"].([]any)
	if rec.Code != 200 || len(accounts) != 1 || accounts[0].(map[string]any)["available_minor"] != float64(7) {
		t.Fatalf("accounts = %d %s", rec.Code, rec.Body)
	}
	svc.listErr = app.ErrLedgerUnavailable
	if rec := do(h, http.MethodGet, "/v1/accounts", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ledger down = %d", rec.Code)
	}

	rec = do(h, http.MethodGet, "/", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Pinkponies Bank") {
		t.Fatalf("UI = %d %s", rec.Code, rec.Body)
	}
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "connect-src 'self' http://localhost:8081") {
		t.Fatalf("CSP = %q", csp)
	}
	if rec := do(h, http.MethodGet, "/config.json", ""); decode(t, rec)["notifications_url"] != "http://localhost:8081" {
		t.Fatalf("config = %s", rec.Body)
	}
}
