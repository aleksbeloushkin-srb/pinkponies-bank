package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
)

// ErrorDomain is the domain of every google.rpc.ErrorInfo the ledger
// returns.
const ErrorDomain = "ledger.pinkponies-bank"

// Reasons of google.rpc.ErrorInfo, part of the API contract (see
// api/proto/ledger/v1/ledger.proto).
const (
	ReasonIdempotencyKeyReused = "IDEMPOTENCY_KEY_REUSED"
	ReasonAccountNotFound      = "ACCOUNT_NOT_FOUND"
	ReasonHoldNotFound         = "HOLD_NOT_FOUND"
	ReasonInsufficientFunds    = "INSUFFICIENT_FUNDS"
	ReasonCurrencyMismatch     = "CURRENCY_MISMATCH"
	ReasonHoldCaptured         = "HOLD_CAPTURED"
	ReasonHoldReleased         = "HOLD_RELEASED"
	ReasonHoldExpired          = "HOLD_EXPIRED"
)

// reasons maps domain errors to their code and reason, in the order they
// are matched.
var reasons = []struct {
	err    error
	code   codes.Code
	reason string
}{
	{ledger.ErrIdempotencyConflict, codes.InvalidArgument, ReasonIdempotencyKeyReused},
	{ledger.ErrAccountNotFound, codes.NotFound, ReasonAccountNotFound},
	{ledger.ErrHoldNotFound, codes.NotFound, ReasonHoldNotFound},
	{ledger.ErrInsufficientFunds, codes.FailedPrecondition, ReasonInsufficientFunds},
	{ledger.ErrCurrencyMismatch, codes.FailedPrecondition, ReasonCurrencyMismatch},
	{ledger.ErrHoldCaptured, codes.FailedPrecondition, ReasonHoldCaptured},
	{ledger.ErrHoldReleased, codes.FailedPrecondition, ReasonHoldReleased},
	{ledger.ErrHoldExpired, codes.FailedPrecondition, ReasonHoldExpired},
}

// toStatus maps an error from the use cases to a gRPC status. It is the
// only place that knows which error means which code. Unexpected errors are
// logged here, once, and the client gets a generic message.
func toStatus(ctx context.Context, log *slog.Logger, err error) error {
	var verr *ledger.ValidationError
	if errors.As(err, &verr) {
		return withDetails(codes.InvalidArgument, verr.Error(), &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: verr.Field, Description: verr.Reason}},
		})
	}
	for _, r := range reasons {
		if errors.Is(err, r.err) {
			return withDetails(r.code, err.Error(), &errdetails.ErrorInfo{Reason: r.reason, Domain: ErrorDomain})
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case pgdb.IsTransient(err):
		// A lost connection, a lock timeout, a deadlock victim: the request
		// may succeed on a retry with the same key.
		log.WarnContext(ctx, "transient failure", "err", err)
		return status.Error(codes.Unavailable, "temporarily unavailable")
	default:
		log.ErrorContext(ctx, "request failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// replayed is the answer to a replay of an idempotency key: AlreadyExists
// with the original result attached, so a client can tell a replay from a
// first execution and still read the result.
func replayed(what string, original protoadapt.MessageV1) error {
	return withDetails(codes.AlreadyExists, what+" already done for this idempotency key; the original result is attached", original)
}

func withDetails(code codes.Code, msg string, details ...protoadapt.MessageV1) error {
	st, err := status.New(code, msg).WithDetails(details...)
	if err != nil {
		// Only fails if a detail cannot be marshalled, which for these
		// generated messages would be a bug; keep the code at least.
		return status.Error(code, msg)
	}
	return st.Err()
}
