// Package httpapi is the transfers service's REST API (api/openapi/
// transfers.yaml): create, read and long-poll transfers, list accounts, and
// serve the web UI.
package httpapi

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

// TransferCreator accepts transfers.
type TransferCreator interface {
	CreateTransfer(ctx context.Context, req app.CreateTransferRequest) (app.CreateTransferResult, error)
}

// TransferReader reads transfers and waits for their changes.
type TransferReader interface {
	Transfer(ctx context.Context, id uuid.UUID) (transfers.Transfer, error)
	WaitTransfer(ctx context.Context, id uuid.UUID, seenVersion int64) (app.WaitResult, error)
}

// AccountLister lists customer accounts.
type AccountLister interface {
	Accounts(ctx context.Context) ([]app.Account, error)
}

// Deps lists what the handlers need. Static is optional.
type Deps struct {
	Creator  TransferCreator
	Reader   TransferReader
	Accounts AccountLister
	// Static is the web UI; nil serves none.
	Static fs.FS
	// NotificationsURL is the notifications API as the browser reaches it,
	// for the UI's feed; empty hides the feed.
	NotificationsURL string
	Metrics          *httpserver.Metrics
	Logger           *slog.Logger
}

// WaitPattern is the long-poll route; its duration is measured apart.
const WaitPattern = "GET /v1/transfers/{id}/wait"

type handler struct {
	creator          TransferCreator
	reader           TransferReader
	accounts         AccountLister
	notificationsURL string
	log              *slog.Logger
}

// NewHandler returns the root handler with every route and middleware.
func NewHandler(d Deps) http.Handler {
	h := &handler{creator: d.Creator, reader: d.Reader, accounts: d.Accounts, notificationsURL: d.NotificationsURL, log: d.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/transfers", h.createTransfer)
	mux.HandleFunc("GET /v1/transfers/{id}", h.transfer)
	mux.HandleFunc(WaitPattern, h.waitTransfer)
	mux.HandleFunc("GET /v1/accounts", h.listAccounts)
	mux.HandleFunc("GET /config.json", h.uiConfig)
	if d.Static != nil {
		mux.Handle("GET /", http.FileServerFS(d.Static))
	}
	return httpserver.Chain(mux,
		httpserver.RequestID,
		httpserver.Logging(d.Logger),
		httpserver.Instrument(d.Metrics),
		httpserver.Recoverer(d.Logger),
		securityHeaders(d.NotificationsURL),
	)
}

// securityHeaders sets a content security policy for the UI: scripts,
// styles and images from this origin only, and fetches to this origin and
// to the notifications API.
func securityHeaders(notificationsURL string) func(http.Handler) http.Handler {
	connect := "'self'"
	if u, err := url.Parse(notificationsURL); err == nil && u.Scheme != "" && u.Host != "" {
		connect += " " + u.Scheme + "://" + u.Host
	}
	csp := "default-src 'self'; connect-src " + connect + "; img-src 'self'; frame-ancestors 'none'; base-uri 'none'"
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Security-Policy", csp)
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Referrer-Policy", "no-referrer")
			next.ServeHTTP(w, r)
		})
	}
}
