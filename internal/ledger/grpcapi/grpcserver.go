package grpcapi

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
)

// NewServer returns a gRPC server with the LedgerService handlers, the
// interceptor chain and the standard health service. The health service
// starts as NOT_SERVING; the caller sets it to SERVING once the server
// listens and back when shutdown starts.
func NewServer(h *Handlers, m *Metrics, log *slog.Logger) (*grpc.Server, *health.Server) {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors(m, log)...),
		// Clients may ping every 10 s; faster pings are an abuse and close
		// the connection.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		// Ping idle clients so a dead peer is noticed in about 40 s, and
		// recycle connections so new instances get a share of the load.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
			MaxConnectionAge:      5 * time.Minute,
			MaxConnectionAgeGrace: 30 * time.Second,
		}),
	)
	ledgerv1.RegisterLedgerServiceServer(srv, h)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	hs.SetServingStatus(ledgerv1.LedgerService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	return srv, hs
}

// SetServing switches the health status of the server and of the ledger
// service.
func SetServing(hs *health.Server, serving bool) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		st = healthpb.HealthCheckResponse_SERVING
	}
	hs.SetServingStatus("", st)
	hs.SetServingStatus(ledgerv1.LedgerService_ServiceDesc.ServiceName, st)
}

// Stop stops srv gracefully, letting in-flight RPCs finish, and stops it
// forcefully if that takes longer than timeout. It reports whether the
// forced stop was needed.
//
// The goroutine running GracefulStop is waited for in both cases. After the
// forced stop it returns once the running handlers return: grpc.Server.Stop
// closes the connections and cancels the handlers' contexts, so a handler
// that respects its context (every database call here does) ends promptly.
func Stop(srv *grpc.Server, timeout time.Duration) (forced bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.GracefulStop()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case <-timer.C:
		srv.Stop()
		<-done
		return true
	}
}
