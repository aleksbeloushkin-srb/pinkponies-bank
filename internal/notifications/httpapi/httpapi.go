// Package httpapi is the notifications service's REST API: an account's
// feed (GET /v1/notifications?account=...), readable from the web UI of
// another origin through CORS, and, in development, a fake webhook
// receiver for the webhook channel.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
)

// Lister reads an account's notifications.
type Lister interface {
	Notifications(ctx context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error)
}

// Deps lists what the handlers need. Sink is optional.
type Deps struct {
	Lister Lister
	// CORSOrigins are the origins allowed to read the API from a browser,
	// such as the transfers web UI.
	CORSOrigins []string
	// Sink, when set, serves the fake webhook receiver under /_sink.
	Sink    *Sink
	Metrics *httpserver.Metrics
	Logger  *slog.Logger
}

type handler struct {
	lister Lister
	log    *slog.Logger
}

// NewHandler returns the root handler with every route and middleware.
func NewHandler(d Deps) http.Handler {
	h := &handler{lister: d.Lister, log: d.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/notifications", h.list)
	if d.Sink != nil {
		d.Sink.register(mux)
	}
	return httpserver.Chain(mux,
		httpserver.RequestID,
		httpserver.Logging(d.Logger),
		httpserver.Instrument(d.Metrics),
		httpserver.Recoverer(d.Logger),
		cors(d.CORSOrigins),
	)
}

type deliveryJSON struct {
	Channel     string     `json:"channel"`
	State       string     `json:"state"`
	Attempts    int        `json:"attempts"`
	LastError   string     `json:"last_error,omitempty"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

type notificationJSON struct {
	ID         uuid.UUID      `json:"id"`
	EventID    uuid.UUID      `json:"event_id"`
	AccountID  uuid.UUID      `json:"account_id"`
	TransferID uuid.UUID      `json:"transfer_id"`
	Kind       string         `json:"kind"`
	Message    string         `json:"message"`
	CreatedAt  time.Time      `json:"created_at"`
	Deliveries []deliveryJSON `json:"deliveries"`
}

// list handles GET /v1/notifications?account=<uuid>&limit=<n>.
func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	account, err := uuid.Parse(r.URL.Query().Get("account"))
	if err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, "invalid_argument", "account must be a UUID", "account")
		return
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 {
			httpserver.WriteError(w, http.StatusBadRequest, "invalid_argument", "limit must be a positive integer", "limit")
			return
		}
	}
	list, err := h.lister.Notifications(r.Context(), account, limit)
	if err != nil {
		if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
			w.WriteHeader(httpserver.StatusClientClosedRequest)
			return
		}
		h.log.ErrorContext(r.Context(), "list notifications failed", "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal", "internal error", "")
		return
	}
	out := struct {
		Notifications []notificationJSON `json:"notifications"`
	}{Notifications: []notificationJSON{}}
	for _, n := range list {
		nj := notificationJSON{ID: n.ID, EventID: n.EventID, AccountID: n.AccountID, TransferID: n.TransferID,
			Kind: string(n.Kind), Message: n.Message, CreatedAt: n.CreatedAt.UTC(), Deliveries: []deliveryJSON{}}
		for _, d := range n.Deliveries {
			dj := deliveryJSON{Channel: d.Channel, State: string(d.State), Attempts: d.Attempts, LastError: d.LastError}
			if !d.DeliveredAt.IsZero() {
				at := d.DeliveredAt.UTC()
				dj.DeliveredAt = &at
			}
			nj.Deliveries = append(nj.Deliveries, dj)
		}
		out.Notifications = append(out.Notifications, nj)
	}
	httpserver.WriteJSON(w, http.StatusOK, out)
}

// cors lets the listed origins read the API from a browser. The feed is a
// simple GET without credentials, so the browser sends no preflight for
// it; one is answered anyway for clients that add headers. An origin that
// is not listed gets no CORS headers, and the browser hides the answer.
func cors(origins []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			allowed := origin != "" && slices.Contains(origins, origin)
			w.Header().Add("Vary", "Origin")
			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Expose-Headers", httpserver.RequestIDHeader)
			}
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				if allowed {
					w.Header().Set("Access-Control-Allow-Methods", "GET")
					w.Header().Set("Access-Control-Allow-Headers", strings.Join([]string{httpserver.RequestIDHeader}, ", "))
					w.Header().Set("Access-Control-Max-Age", "600")
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
