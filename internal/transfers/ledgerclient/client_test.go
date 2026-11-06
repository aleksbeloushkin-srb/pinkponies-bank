package ledgerclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/protoadapt"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

// fakeLedger answers every RPC with the scripted response or error and
// records what it saw.
type fakeLedger struct {
	ledgerv1.UnimplementedLedgerServiceServer
	mu        sync.Mutex
	err       error
	hold      *ledgerv1.PlaceHoldResponse
	pages     []*ledgerv1.ListAccountsResponse
	block     bool
	deadline  time.Duration
	requestID string
	tokens    []string
}

func (f *fakeLedger) observe(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(requestIDKey)) > 0 {
		f.requestID = md.Get(requestIDKey)[0]
	}
}

func (f *fakeLedger) PlaceHold(ctx context.Context, _ *ledgerv1.PlaceHoldRequest) (*ledgerv1.PlaceHoldResponse, error) {
	f.observe(ctx)
	if f.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.hold, nil
}

func (f *fakeLedger) ListAccounts(_ context.Context, req *ledgerv1.ListAccountsRequest) (*ledgerv1.ListAccountsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = append(f.tokens, req.GetPageToken())
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func start(t *testing.T, srv *fakeLedger, timeout time.Duration) (*Client, *Metrics) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	m := NewMetrics(prometheus.NewRegistry())
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(propagateRequestID, m.instrument))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return New(conn, Config{CallTimeout: timeout}), m
}

func withReason(code codes.Code, reason ledgerv1.ErrorReason) error {
	st, _ := status.New(code, "refused").WithDetails(&errdetails.ErrorInfo{Reason: reason.String(), Domain: "ledger.pinkponies-bank"})
	return st.Err()
}

func withDetail(code codes.Code, d protoadapt.MessageV1) error {
	st, _ := status.New(code, "replay").WithDetails(d)
	return st.Err()
}

func holdRequest() app.HoldRequest {
	return app.HoldRequest{Key: "transfer:1:hold", SourceID: uuid.New(), DestinationID: uuid.New(), AmountMinor: 100, Currency: "EUR", TTL: time.Minute}
}

func TestStatusCodesMapToErrorClasses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"insufficient funds", withReason(codes.FailedPrecondition, ledgerv1.ErrorReason_INSUFFICIENT_FUNDS), app.ErrInsufficientFunds},
		{"currency", withReason(codes.FailedPrecondition, ledgerv1.ErrorReason_CURRENCY_MISMATCH), app.ErrCurrencyMismatch},
		{"expired", withReason(codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_EXPIRED), app.ErrHoldExpired},
		{"released", withReason(codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_RELEASED), app.ErrHoldExpired},
		{"captured", withReason(codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_CAPTURED), app.ErrHoldCaptured},
		{"unknown account", withReason(codes.NotFound, ledgerv1.ErrorReason_ACCOUNT_NOT_FOUND), app.ErrAccountNotFound},
		{"unknown hold", withReason(codes.NotFound, ledgerv1.ErrorReason_HOLD_NOT_FOUND), app.ErrLedgerContract},
		{"precondition without a reason", status.Error(codes.FailedPrecondition, "?"), app.ErrLedgerContract},
		{"invalid argument", withReason(codes.InvalidArgument, ledgerv1.ErrorReason_IDEMPOTENCY_KEY_REUSED), app.ErrLedgerContract},
		{"unavailable", status.Error(codes.Unavailable, "down"), app.ErrLedgerUnavailable},
		{"aborted", status.Error(codes.Aborted, "deadlock victim"), app.ErrLedgerUnavailable},
		{"internal", status.Error(codes.Internal, "bug"), app.ErrLedgerUnavailable},
		{"canceled", status.Error(codes.Canceled, "server gave up"), app.ErrLedgerUnavailable},
		{"replay without the original", status.Error(codes.AlreadyExists, "replay"), app.ErrLedgerContract},
		{"unimplemented", status.Error(codes.Unimplemented, "old server"), app.ErrLedgerContract},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := start(t, &fakeLedger{err: tt.err}, time.Second)
			_, err := c.PlaceHold(context.Background(), holdRequest())
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// A call whose caller has gone keeps the class the saga retries on, and
// also the caller's cause, so an API handler can tell a client that left
// from a ledger that is down.
func TestCallerCancelKeepsItsCause(t *testing.T) {
	c, _ := start(t, &fakeLedger{block: true}, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.PlaceHold(ctx, holdRequest())
	if !errors.Is(err, app.ErrLedgerUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrLedgerUnavailable and context.Canceled", err)
	}
	c, _ = start(t, &fakeLedger{err: status.Error(codes.Unavailable, "down")}, time.Minute)
	if _, err := c.PlaceHold(context.Background(), holdRequest()); errors.Is(err, context.Canceled) {
		t.Fatalf("a live caller's failure = %v must not look cancelled", err)
	}
}

// AlreadyExists with the original response attached is a success marked as
// a replay.
func TestReplayReturnsTheOriginalResult(t *testing.T) {
	holdID := uuid.New()
	original := &ledgerv1.PlaceHoldResponse{Hold: &ledgerv1.Hold{Id: holdID.String()}}
	c, _ := start(t, &fakeLedger{err: withDetail(codes.AlreadyExists, original)}, time.Second)
	hold, err := c.PlaceHold(context.Background(), holdRequest())
	if err != nil || !hold.Replayed || hold.ID != holdID {
		t.Fatalf("PlaceHold = %+v, %v", hold, err)
	}

	c, _ = start(t, &fakeLedger{hold: original}, time.Second)
	hold, err = c.PlaceHold(context.Background(), holdRequest())
	if err != nil || hold.Replayed || hold.ID != holdID {
		t.Fatalf("first execution = %+v, %v", hold, err)
	}
}

// Every RPC carries its own deadline; an expired one is a retryable
// ambiguity, and the request id travels with the call.
func TestPerCallDeadlineAndRequestID(t *testing.T) {
	srv := &fakeLedger{block: true}
	c, m := start(t, srv, time.Second)
	ctx := platform.WithRequestID(context.Background(), "transfer-1")
	_, err := c.PlaceHold(ctx, holdRequest())
	if !errors.Is(err, app.ErrLedgerUnavailable) {
		t.Fatalf("err = %v, want ErrLedgerUnavailable", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.deadline <= 0 || srv.deadline > time.Second {
		t.Fatalf("server saw a deadline of %s, want at most the call timeout", srv.deadline)
	}
	if srv.requestID != "transfer-1" {
		t.Fatalf("request id = %q", srv.requestID)
	}
	method := ledgerv1.LedgerService_PlaceHold_FullMethodName
	if got := testutil.ToFloat64(m.handled.WithLabelValues(method, codes.DeadlineExceeded.String())); got != 1 {
		t.Fatalf("grpc_client_handled_total{DeadlineExceeded} = %v", got)
	}
}

func TestAccountsPagesAndKeepsCustomersOnly(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	srv := &fakeLedger{pages: []*ledgerv1.ListAccountsResponse{
		{Accounts: []*ledgerv1.Account{
			{Id: a.String(), Name: "Clover", Kind: ledgerv1.AccountKind_ACCOUNT_KIND_CUSTOMER, AvailableMinor: 5},
			{Id: uuid.NewString(), Name: "Treasury EUR", Kind: ledgerv1.AccountKind_ACCOUNT_KIND_SYSTEM},
		}, NextPageToken: "next"},
		{Accounts: []*ledgerv1.Account{{Id: b.String(), Name: "Pip", Kind: ledgerv1.AccountKind_ACCOUNT_KIND_CUSTOMER}}},
	}}
	c, _ := start(t, srv, time.Second)
	accounts, err := c.Accounts(context.Background())
	if err != nil || len(accounts) != 2 || accounts[0].ID != a || accounts[1].ID != b || accounts[0].AvailableMinor != 5 {
		t.Fatalf("Accounts = %+v, %v", accounts, err)
	}
	if len(srv.tokens) != 2 || srv.tokens[0] != "" || srv.tokens[1] != "next" {
		t.Fatalf("page tokens sent = %q", srv.tokens)
	}
}
