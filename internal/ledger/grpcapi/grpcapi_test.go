package grpcapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// fakeService answers every use case from function fields; a nil field
// panics, which the recovery interceptor turns into Internal.
type fakeService struct {
	placeHold   func(app.PlaceHoldRequest) (app.HoldResult, error)
	captureHold func(app.CaptureHoldRequest) (app.CaptureResult, error)
	releaseHold func(app.ReleaseHoldRequest) (app.HoldResult, error)
	account     func(context.Context, uuid.UUID) (ledger.Account, error)
}

func (f *fakeService) OpenAccount(context.Context, app.OpenAccountRequest) (app.OpenAccountResult, error) {
	return app.OpenAccountResult{}, errors.New("not used")
}

func (f *fakeService) Account(ctx context.Context, id uuid.UUID) (ledger.Account, error) {
	return f.account(ctx, id)
}

func (f *fakeService) Accounts(context.Context, int, string) (app.Page[ledger.Account], error) {
	return app.Page[ledger.Account]{}, nil
}

func (f *fakeService) PlaceHold(_ context.Context, req app.PlaceHoldRequest) (app.HoldResult, error) {
	return f.placeHold(req)
}

func (f *fakeService) CaptureHold(_ context.Context, req app.CaptureHoldRequest) (app.CaptureResult, error) {
	return f.captureHold(req)
}

func (f *fakeService) ReleaseHold(_ context.Context, req app.ReleaseHoldRequest) (app.HoldResult, error) {
	return f.releaseHold(req)
}

func (f *fakeService) Entries(context.Context, uuid.UUID, int, string) (app.Page[ledger.Entry], error) {
	return app.Page[ledger.Entry]{}, nil
}

type harness struct {
	client  ledgerv1.LedgerServiceClient
	health  healthpb.HealthClient
	hs      *health.Server
	srv     *grpc.Server
	metrics *Metrics
	logs    *syncBuffer
}

// syncBuffer is a bytes.Buffer safe for the server's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func start(t *testing.T, svc Service) *harness {
	t.Helper()
	h := &harness{logs: &syncBuffer{}, metrics: NewMetrics(prometheus.NewRegistry())}
	log := platform.NewLogger(h.logs, slog.LevelInfo)
	h.srv, h.hs = NewServer(NewHandlers(svc, log), h.metrics, log)
	lis := bufconn.Listen(1 << 20)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = h.srv.Serve(lis)
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		h.srv.Stop()
		<-served
	})
	h.client = ledgerv1.NewLedgerServiceClient(conn)
	h.health = healthpb.NewHealthClient(conn)
	return h
}

func reasonOf(t *testing.T, err error) (codes.Code, string) {
	t.Helper()
	st := status.Convert(err)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			if info.GetDomain() != ErrorDomain {
				t.Fatalf("ErrorInfo domain = %q", info.GetDomain())
			}
			return st.Code(), info.GetReason()
		}
	}
	return st.Code(), ""
}

func validHoldRequest() *ledgerv1.PlaceHoldRequest {
	return &ledgerv1.PlaceHoldRequest{
		IdempotencyKey: "transfer:1:hold", AccountId: uuid.NewString(), DestinationAccountId: uuid.NewString(),
		AmountMinor: 100, Currency: "EUR",
	}
}

// Every domain error reaches the client as one code and one reason.
func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		code   codes.Code
		reason string
	}{
		{fmt.Errorf("source: %w", ledger.ErrAccountNotFound), codes.NotFound, ReasonAccountNotFound},
		{ledger.ErrHoldNotFound, codes.NotFound, ReasonHoldNotFound},
		{fmt.Errorf("%w: available 1", ledger.ErrInsufficientFunds), codes.FailedPrecondition, ReasonInsufficientFunds},
		{ledger.ErrCurrencyMismatch, codes.FailedPrecondition, ReasonCurrencyMismatch},
		{ledger.ErrHoldCaptured, codes.FailedPrecondition, ReasonHoldCaptured},
		{ledger.ErrHoldReleased, codes.FailedPrecondition, ReasonHoldReleased},
		{ledger.ErrHoldExpired, codes.FailedPrecondition, ReasonHoldExpired},
		{ledger.ErrIdempotencyConflict, codes.InvalidArgument, ReasonIdempotencyKeyReused},
		{&pgconn.PgError{Code: "40P01"}, codes.Unavailable, ""},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), codes.DeadlineExceeded, ""},
		{errors.New("disk on fire"), codes.Internal, ""},
	}
	for _, tt := range tests {
		t.Run(tt.code.String()+" "+tt.reason, func(t *testing.T) {
			h := start(t, &fakeService{placeHold: func(app.PlaceHoldRequest) (app.HoldResult, error) {
				return app.HoldResult{}, tt.err
			}})
			_, err := h.client.PlaceHold(context.Background(), validHoldRequest())
			code, reason := reasonOf(t, err)
			if code != tt.code || reason != tt.reason {
				t.Fatalf("got %s %q, want %s %q", code, reason, tt.code, tt.reason)
			}
			if code == codes.Internal && strings.Contains(status.Convert(err).Message(), "disk") {
				t.Fatal("an internal error must not leak its cause to the client")
			}
		})
	}
}

func TestValidationErrorCarriesTheField(t *testing.T) {
	h := start(t, &fakeService{})
	req := validHoldRequest()
	req.AccountId = "not-a-uuid"
	_, err := h.client.PlaceHold(context.Background(), req)
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %s", st.Code())
	}
	for _, d := range st.Details() {
		if br, ok := d.(*errdetails.BadRequest); ok && br.GetFieldViolations()[0].GetField() == "account_id" {
			return
		}
	}
	t.Fatalf("details = %v, want a BadRequest for account_id", st.Details())
}

func TestNegativeTTLIsInvalid(t *testing.T) {
	h := start(t, &fakeService{})
	req := validHoldRequest()
	req.Ttl = durationpb.New(-time.Second)
	if _, err := h.client.PlaceHold(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

// A replay is AlreadyExists with the original response attached.
func TestReplayIsAlreadyExistsWithTheOriginalResult(t *testing.T) {
	holdID := uuid.New()
	txID := uuid.New()
	h := start(t, &fakeService{captureHold: func(req app.CaptureHoldRequest) (app.CaptureResult, error) {
		return app.CaptureResult{
			Hold:        ledger.Hold{ID: req.HoldID, Status: ledger.HoldCaptured, TransactionID: txID},
			Transaction: ledger.Transaction{ID: txID, Kind: ledger.KindTransfer, HoldID: req.HoldID},
			Replayed:    true,
		}, nil
	}})
	_, err := h.client.CaptureHold(context.Background(), &ledgerv1.CaptureHoldRequest{IdempotencyKey: "k", HoldId: holdID.String()})
	st := status.Convert(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("code = %s, want AlreadyExists", st.Code())
	}
	var original *ledgerv1.CaptureHoldResponse
	for _, d := range st.Details() {
		if r, ok := d.(*ledgerv1.CaptureHoldResponse); ok {
			original = r
		}
	}
	if original.GetTransaction().GetId() != txID.String() || original.GetHold().GetStatus() != ledgerv1.HoldStatus_HOLD_STATUS_CAPTURED {
		t.Fatalf("attached result = %v", original)
	}
}

func TestFirstExecutionIsOK(t *testing.T) {
	h := start(t, &fakeService{placeHold: func(req app.PlaceHoldRequest) (app.HoldResult, error) {
		if req.TTL != 2*time.Minute {
			t.Errorf("TTL = %s", req.TTL)
		}
		return app.HoldResult{Hold: ledger.Hold{ID: uuid.New(), AccountID: req.AccountID, Status: ledger.HoldActive}}, nil
	}})
	req := validHoldRequest()
	req.Ttl = durationpb.New(2 * time.Minute)
	resp, err := h.client.PlaceHold(context.Background(), req)
	if err != nil || resp.GetHold().GetStatus() != ledgerv1.HoldStatus_HOLD_STATUS_ACTIVE || resp.GetHold().GetAccountId() != req.GetAccountId() {
		t.Fatalf("PlaceHold = %v, %v", resp, err)
	}
}

// A panic becomes Internal; the access log, the metrics and the request id
// all see it.
func TestPanicIsRecoveredAndObserved(t *testing.T) {
	h := start(t, &fakeService{}) // account is nil: GetAccount panics
	ctx := metadata.AppendToOutgoingContext(context.Background(), RequestIDMetadataKey, "req-42")
	var header metadata.MD
	_, err := h.client.GetAccount(ctx, &ledgerv1.GetAccountRequest{AccountId: uuid.NewString()}, grpc.Header(&header))

	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
	if got := header.Get(RequestIDMetadataKey); len(got) != 1 || got[0] != "req-42" {
		t.Fatalf("response request id = %v", got)
	}
	if got := testutil.ToFloat64(h.metrics.panics); got != 1 {
		t.Fatalf("panics = %v", got)
	}
	method := ledgerv1.LedgerService_GetAccount_FullMethodName
	if got := testutil.ToFloat64(h.metrics.handled.WithLabelValues(method, "Internal")); got != 1 {
		t.Fatalf("handled{Internal} = %v", got)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, `"msg":"panic recovered"`) || !strings.Contains(logs, `"code":"Internal"`) ||
		strings.Count(logs, `"request_id":"req-42"`) != 2 {
		t.Fatalf("logs:\n%s", logs)
	}
}

func TestHealthFollowsServing(t *testing.T) {
	h := start(t, &fakeService{})
	check := func() healthpb.HealthCheckResponse_ServingStatus {
		resp, err := h.health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ledgerv1.LedgerService_ServiceDesc.ServiceName})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetStatus()
	}
	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("before start: %s", got)
	}
	SetServing(h.hs, true)
	if got := check(); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("after start: %s", got)
	}
}

// Stop forces a server whose RPC does not finish within the timeout. The
// forced stop cancels the handler's context, and Stop returns once the
// handler has returned.
func TestStopGracefulThenForced(t *testing.T) {
	entered := make(chan struct{})
	h := start(t, &fakeService{account: func(ctx context.Context, _ uuid.UUID) (ledger.Account, error) {
		close(entered)
		<-ctx.Done()
		return ledger.Account{}, ctx.Err()
	}})
	done := make(chan error, 1)
	go func() {
		_, err := h.client.GetAccount(context.Background(), &ledgerv1.GetAccountRequest{AccountId: uuid.NewString()})
		done <- err
	}()
	<-entered

	if forced := Stop(h.srv, 50*time.Millisecond); !forced {
		t.Fatal("a stuck RPC must make Stop force the server down")
	}
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("client of a forced stop got %v, want Unavailable", err)
	}
}

func TestStopWithoutRPCsIsGraceful(t *testing.T) {
	h := start(t, &fakeService{})
	if forced := Stop(h.srv, time.Second); forced {
		t.Fatal("an idle server must stop gracefully")
	}
}
