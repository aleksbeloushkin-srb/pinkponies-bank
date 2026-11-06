// Package ledgerclient is the transfers service's gRPC client of the
// ledger: it implements app.Ledger.
//
// Every call gets its own deadline, the earlier of CallTimeout and the
// caller's, so one slow RPC cannot use up a saga step's whole task timeout.
// Status codes are mapped to the error classes of the app package here and
// nowhere else:
//
//	OK, AlreadyExists (with the original      success; Replayed is set
//	  response attached)                      for AlreadyExists
//	FailedPrecondition, NotFound              a refusal, by ErrorInfo reason
//	Unavailable, DeadlineExceeded, Aborted,   app.ErrLedgerUnavailable: the
//	  ResourceExhausted, Internal, Unknown,   request may or may not have run;
//	  Canceled                                retry with the same key
//	InvalidArgument and anything else         app.ErrLedgerContract
//
// Internal is retried although it usually means a bug: the client cannot
// tell a bug from a transient fault behind it, retrying with the same key
// is safe, and the task's attempt cap bounds the cost.
//
// There are no retries in here. grpc-go retries transparently only RPCs
// that never reached the server; everything else is retried by the task
// queue, which owns the backoff and the attempt count.
package ledgerclient

import (
	"context"

	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

// Config tunes the client.
type Config struct {
	// CallTimeout bounds one RPC.
	CallTimeout time.Duration
}

// Client calls the ledger.
type Client struct {
	api ledgerv1.LedgerServiceClient
	cfg Config
}

// New returns a Client over conn, usually from Dial.
func New(conn grpc.ClientConnInterface, cfg Config) *Client {
	return &Client{api: ledgerv1.NewLedgerServiceClient(conn), cfg: cfg}
}

// PlaceHold implements app.Ledger.
func (c *Client) PlaceHold(ctx context.Context, req app.HoldRequest) (app.Hold, error) {
	resp, replayed, err := call(ctx, c, c.api.PlaceHold, &ledgerv1.PlaceHoldRequest{
		IdempotencyKey:       req.Key,
		AccountId:            req.SourceID.String(),
		DestinationAccountId: req.DestinationID.String(),
		AmountMinor:          req.AmountMinor,
		Currency:             req.Currency,
		Ttl:                  durationpb.New(req.TTL),
	})
	if err != nil {
		return app.Hold{}, err
	}
	id, err := parseID(resp.GetHold().GetId())
	if err != nil {
		return app.Hold{}, err
	}
	return app.Hold{ID: id, Replayed: replayed}, nil
}

// CaptureHold implements app.Ledger.
func (c *Client) CaptureHold(ctx context.Context, holdID uuid.UUID, key string) (app.Capture, error) {
	resp, replayed, err := call(ctx, c, c.api.CaptureHold, &ledgerv1.CaptureHoldRequest{
		IdempotencyKey: key, HoldId: holdID.String(),
	})
	if err != nil {
		return app.Capture{}, err
	}
	id, err := parseID(resp.GetTransaction().GetId())
	if err != nil {
		return app.Capture{}, err
	}
	return app.Capture{TransactionID: id, Replayed: replayed}, nil
}

// ReleaseHold implements app.Ledger.
func (c *Client) ReleaseHold(ctx context.Context, holdID uuid.UUID, key, reason string) (app.Release, error) {
	_, replayed, err := call(ctx, c, c.api.ReleaseHold, &ledgerv1.ReleaseHoldRequest{
		IdempotencyKey: key, HoldId: holdID.String(), Reason: reason,
	})
	if err != nil {
		return app.Release{}, err
	}
	return app.Release{Replayed: replayed}, nil
}

// maxAccountPages bounds Accounts: a demo bank has a page or two, and a
// runaway pagination must not turn one page view into thousands of RPCs.
const maxAccountPages = 20

// Accounts implements app.Ledger: every customer account, page by page.
func (c *Client) Accounts(ctx context.Context) ([]app.Account, error) {
	var (
		out   []app.Account
		token string
	)
	for range maxAccountPages {
		resp, _, err := call(ctx, c, c.api.ListAccounts, &ledgerv1.ListAccountsRequest{PageSize: 200, PageToken: token})
		if err != nil {
			return nil, err
		}
		for _, a := range resp.GetAccounts() {
			if a.GetKind() != ledgerv1.AccountKind_ACCOUNT_KIND_CUSTOMER {
				continue
			}
			id, err := parseID(a.GetId())
			if err != nil {
				return nil, err
			}
			out = append(out, app.Account{
				ID:             id,
				Name:           a.GetName(),
				Currency:       a.GetCurrency(),
				PostedMinor:    a.GetPostedMinor(),
				HeldMinor:      a.GetHeldMinor(),
				AvailableMinor: a.GetAvailableMinor(),
			})
		}
		if token = resp.GetNextPageToken(); token == "" {
			return out, nil
		}
	}
	return nil, fmt.Errorf("%w: more than %d pages of accounts", app.ErrLedgerContract, maxAccountPages)
}

// call runs one RPC with its own deadline. A replay (AlreadyExists) is
// turned into the original response attached to it. When the caller's
// context ended first, the error wraps its cause as well, so the caller
// can tell "the client has gone" from "the ledger is down".
func call[Req, Resp proto.Message](ctx context.Context, c *Client,
	rpc func(context.Context, Req, ...grpc.CallOption) (Resp, error), req Req) (Resp, bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	resp, err := rpc(callCtx, req)
	if err == nil {
		return resp, false, nil
	}
	if original, ok := replayOf[Resp](err); ok {
		return original, true, nil
	}
	var zero Resp
	if cause := context.Cause(ctx); cause != nil {
		return zero, false, fmt.Errorf("%w: %w", classify(err), cause)
	}
	return zero, false, classify(err)
}

// replayOf extracts the original response from an AlreadyExists status.
func replayOf[Resp proto.Message](err error) (Resp, bool) {
	var zero Resp
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		return zero, false
	}
	for _, d := range st.Details() {
		if m, ok := d.(Resp); ok {
			return m, true
		}
	}
	return zero, false
}

// refusals maps ErrorInfo reasons to the app's error classes.
var refusals = map[string]error{
	ledgerv1.ErrorReason_INSUFFICIENT_FUNDS.String(): app.ErrInsufficientFunds,
	ledgerv1.ErrorReason_ACCOUNT_NOT_FOUND.String():  app.ErrAccountNotFound,
	ledgerv1.ErrorReason_CURRENCY_MISMATCH.String():  app.ErrCurrencyMismatch,
	ledgerv1.ErrorReason_HOLD_EXPIRED.String():       app.ErrHoldExpired,
	ledgerv1.ErrorReason_HOLD_RELEASED.String():      app.ErrHoldExpired,
	ledgerv1.ErrorReason_HOLD_CAPTURED.String():      app.ErrHoldCaptured,
}

// classify maps a failed RPC to an app error class, keeping the code and
// message for the logs.
func classify(err error) error {
	st := status.Convert(err)
	wrap := func(class error) error { return fmt.Errorf("%w: %s: %s", class, st.Code(), st.Message()) }
	switch st.Code() {
	case codes.FailedPrecondition, codes.NotFound:
		if class, ok := refusals[reasonOf(st)]; ok {
			return wrap(class)
		}
		return wrap(app.ErrLedgerContract)
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.ResourceExhausted,
		codes.Internal, codes.Unknown, codes.Canceled:
		return wrap(app.ErrLedgerUnavailable)
	default:
		return wrap(app.ErrLedgerContract)
	}
}

func reasonOf(st *status.Status) string {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: malformed id %q in a response", app.ErrLedgerContract, s)
	}
	return id, nil
}
