package ledgerclient

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// requestIDKey carries the request id to the ledger, which logs it.
const requestIDKey = "x-request-id"

// Metrics are the client-side gRPC metrics: what the ledger looks like from
// here, including deadlines that expired on the way.
type Metrics struct {
	handled  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics creates the client metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_client_handled_total",
			Help: "Unary RPCs to the ledger, by full method name and status code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_client_handling_seconds",
			Help:    "Duration of unary RPCs to the ledger, by full method name.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method"}),
	}
	reg.MustRegister(m.handled, m.duration)
	for _, md := range ledgerv1.LedgerService_ServiceDesc.Methods {
		m.handled.WithLabelValues("/"+ledgerv1.LedgerService_ServiceDesc.ServiceName+"/"+md.MethodName, "OK")
	}
	return m
}

// Dial returns a connection to the ledger at target, for example
// "dns:///ledger:50051". The connection is lazy: it connects on the first
// RPC and reconnects on its own.
//
// The transport is plaintext, for a private network only; production would
// use mTLS. Keepalive pings notice a dead ledger behind a silent network
// within about 40 s; the ledger's enforcement policy allows them.
func Dial(target string, m *Metrics) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(propagateRequestID, m.instrument),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
		// Spread RPCs over every ledger instance the name resolves to.
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin": {}}]}`),
	)
}

func propagateRequestID(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if id := platform.RequestID(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, requestIDKey, id)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func (m *Metrics) instrument(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	m.duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	m.handled.WithLabelValues(method, status.Code(err).String()).Inc()
	return err
}
