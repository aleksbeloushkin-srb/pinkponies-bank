package platform

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"regexp"
)

type requestIDKey struct{}

// WithRequestID returns a ctx that carries the request id. The logger adds
// it to every record logged with that ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// ValidRequestID reports whether an incoming request id may be kept: short,
// and made of characters that are safe in logs and headers.
func ValidRequestID(id string) bool { return validRequestID.MatchString(id) }

// NewRequestID returns a fresh random request id.
func NewRequestID() string { return rand.Text() }

// RequestID returns the request id carried by ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// NewLogger returns a JSON logger that writes to w and adds the request id
// from the context to each record. Call the *Context logging methods
// (InfoContext, ErrorContext) to get the id.
//
// The logger stays an explicit dependency passed to constructors; only
// request-scoped data (the id) travels in the context.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(contextHandler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})})
}

type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
