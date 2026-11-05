package httpserver

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// Chain wraps h so that the first middleware is the outermost.
func Chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// RequestIDHeader carries the request id in both directions.
const RequestIDHeader = "X-Request-ID"

// RequestID puts a request id in the context and the response. A
// well-formed incoming id is kept so a caller can correlate its logs with
// ours; anything else is replaced. Behind a public edge, let the edge proxy
// overwrite the header so clients cannot inject ids.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !platform.ValidRequestID(id) {
			id = platform.NewRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(platform.WithRequestID(r.Context(), id)))
	})
}

// statusWriter records the status code and body size.
type statusWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer (Flush,
// SetWriteDeadline) through this wrapper.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Logging writes one access-log line per request. It logs the route pattern
// and the path, never the query string or the body.
func Logging(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			log.InfoContext(r.Context(), "http request",
				"method", r.Method,
				"route", r.Pattern, // set by ServeMux on this same *Request
				"path", r.URL.Path,
				"status", sw.status,
				"bytes", sw.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// Recoverer turns a handler panic into a logged 500. It sits inside Logging
// and Instrument so both record the 500. http.ErrAbortHandler is
// re-panicked: it is net/http's way to abort a response on purpose.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w}
			defer func() {
				p := recover()
				if p == nil {
					return
				}
				if p == http.ErrAbortHandler {
					panic(p)
				}
				log.ErrorContext(r.Context(), "panic recovered", "panic", p, "stack", string(debug.Stack()))
				if !sw.wroteHeader {
					WriteError(sw, http.StatusInternalServerError, "internal", "internal error", "")
				}
			}()
			next.ServeHTTP(sw, r)
		})
	}
}
