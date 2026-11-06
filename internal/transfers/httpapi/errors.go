package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/longpoll"
)

// writeError maps an error to an HTTP response. It is the only place that
// knows which error means which status code. Unexpected errors are logged
// here, once, and the client gets a generic message without internals.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var (
		verr     *transfers.ValidationError
		berr     *httpserver.BodyError
		maxBytes *http.MaxBytesError
	)
	switch {
	case r.Context().Err() != nil:
		// The client has gone, and whatever failed failed because of it, or
		// nobody reads the answer anyway. 499 only reaches the access log
		// and the metrics, not the 5xx rate.
		log.DebugContext(r.Context(), "client went away", "err", err)
		w.WriteHeader(httpserver.StatusClientClosedRequest)
	case errors.As(err, &verr):
		httpserver.WriteError(w, http.StatusBadRequest, "invalid_argument", verr.Error(), verr.Field)
	case errors.As(err, &berr):
		httpserver.WriteError(w, http.StatusBadRequest, "invalid_argument", berr.Error(), "body")
	case errors.As(err, &maxBytes):
		httpserver.WriteError(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large", "")
	case errors.Is(err, transfers.ErrNotFound):
		httpserver.WriteError(w, http.StatusNotFound, "not_found", err.Error(), "")
	case errors.Is(err, transfers.ErrIdempotencyConflict):
		httpserver.WriteError(w, http.StatusUnprocessableEntity, "idempotency_conflict", err.Error(), "")
	case errors.Is(err, longpoll.ErrTooManyWaiters):
		w.Header().Set("Retry-After", "1")
		httpserver.WriteError(w, http.StatusTooManyRequests, "too_many_waiters", "too many open long polls, retry shortly", "")
	case errors.Is(err, longpoll.ErrClosed):
		w.Header().Set("Retry-After", "1")
		httpserver.WriteError(w, http.StatusServiceUnavailable, "unavailable", "shutting down, retry shortly", "")
	case errors.Is(err, app.ErrLedgerUnavailable):
		log.WarnContext(r.Context(), "ledger unavailable", "err", err)
		w.Header().Set("Retry-After", "1")
		httpserver.WriteError(w, http.StatusServiceUnavailable, "upstream_unavailable", "the ledger is unavailable, retry shortly", "")
	default:
		log.ErrorContext(r.Context(), "request failed", "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal", "internal error", "")
	}
}
