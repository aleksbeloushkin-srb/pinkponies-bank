package grpcapi

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// RequestIDMetadataKey carries the request id in gRPC metadata, both ways.
const RequestIDMetadataKey = "x-request-id"

// Metrics are the gRPC server's metrics. The method label is the full
// method name of a registered method, a small fixed set.
type Metrics struct {
	handled  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	panics   prometheus.Counter
}

// NewMetrics creates the server metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_handled_total",
			Help: "Unary RPCs completed, by full method name and status code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Duration of unary RPCs, by full method name.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"method"}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "grpc_server_panics_total",
			Help: "Handler panics recovered and answered with Internal.",
		}),
	}
	reg.MustRegister(m.handled, m.duration, m.panics)
	for _, md := range ledgerv1.LedgerService_ServiceDesc.Methods {
		m.handled.WithLabelValues("/"+ledgerv1.LedgerService_ServiceDesc.ServiceName+"/"+md.MethodName, codes.OK.String())
	}
	return m
}

// interceptors returns the server's unary chain, outermost first: the
// request id reaches every log line, the access log and the metrics see the
// status that recovery made of a panic.
func interceptors(m *Metrics, log *slog.Logger) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{requestID, accessLog(log), instrument(m), recovery(m, log)}
}

// requestID keeps a well-formed incoming request id, or makes one, puts it
// in the context for the logger and sends it back in the response header.
func requestID(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var id string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(RequestIDMetadataKey); len(v) > 0 {
			id = v[0]
		}
	}
	if !platform.ValidRequestID(id) {
		id = platform.NewRequestID()
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDMetadataKey, id))
	return handler(platform.WithRequestID(ctx, id), req)
}

// accessLog writes one line per RPC with the method, the code and the
// duration; never the request, which holds account ids and amounts.
func accessLog(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		log.InfoContext(ctx, "grpc request",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return resp, err
	}
}

func instrument(m *Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.duration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		m.handled.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		return resp, err
	}
}

// recovery turns a handler panic into Internal. It is the innermost
// interceptor, so logging and metrics see the Internal it became.
func recovery(m *Metrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				m.panics.Inc()
				log.ErrorContext(ctx, "panic recovered", "method", info.FullMethod, "panic", p, "stack", string(debug.Stack()))
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}
