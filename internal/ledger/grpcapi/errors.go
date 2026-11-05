package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
)

// ErrorDomain is the domain of every google.rpc.ErrorInfo the ledger
// returns.
const ErrorDomain = "ledger.pinkponies-bank"

// reasons maps domain errors to their code and ErrorInfo reason, in the
// order they are matched. The reasons are part of the contract: the
// ErrorReason enum of api/proto/ledger/v1/ledger.proto.
var reasons = []struct {
	err    error
	code   codes.Code
	reason ledgerv1.ErrorReason
}{
	{ledger.ErrIdempotencyConflict, codes.InvalidArgument, ledgerv1.ErrorReason_IDEMPOTENCY_KEY_REUSED},
	{ledger.ErrAccountNotFound, codes.NotFound, ledgerv1.ErrorReason_ACCOUNT_NOT_FOUND},
	{ledger.ErrHoldNotFound, codes.NotFound, ledgerv1.ErrorReason_HOLD_NOT_FOUND},
	{ledger.ErrInsufficientFunds, codes.FailedPrecondition, ledgerv1.ErrorReason_INSUFFICIENT_FUNDS},
	{ledger.ErrCurrencyMismatch, codes.FailedPrecondition, ledgerv1.ErrorReason_CURRENCY_MISMATCH},
	{ledger.ErrHoldCaptured, codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_CAPTURED},
	{ledger.ErrHoldReleased, codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_RELEASED},
	{ledger.ErrHoldExpired, codes.FailedPrecondition, ledgerv1.ErrorReason_HOLD_EXPIRED},
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
			return withDetails(r.code, err.Error(), &errdetails.ErrorInfo{Reason: r.reason.String(), Domain: ErrorDomain})
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
