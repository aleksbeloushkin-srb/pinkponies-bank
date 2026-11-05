// Command ledger is the Pinkponies Bank ledger: accounts, holds and the
// double-entry journal behind a gRPC API, with an expiry sweep for
// forgotten holds.
//
// main is the composition root. It is the only place that knows every
// concrete type: it reads the configuration, builds the adapters, injects
// them into the use cases, starts the components and stops them in the
// right order. Read it top to bottom.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/grpcapi"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/metrics"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/migrations"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// metricsQueryTimeout bounds the database query behind the gauges, well
// below the default Prometheus scrape timeout of 10 s.
const metricsQueryTimeout = 2 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv(envPrefix + "ADMIN_ADDR")
		if addr == "" {
			addr = ":9101"
		}
		os.Exit(platform.Healthcheck(addr))
	}

	// Catch signals before anything else, so a SIGTERM during start-up
	// still goes through the orderly path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig(os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, "ledger: config:", err)
		os.Exit(1)
	}
	log := platform.NewLogger(os.Stdout, cfg.LogLevel)

	if err := run(ctx, stop, cfg, log); err != nil {
		log.Error("ledger stopped with an error", "err", err)
		os.Exit(1)
	}
	log.Info("ledger stopped")
}

// run builds the ledger, serves until ctx is cancelled or a component
// fails, and then shuts down in order:
//
//  1. readiness and the gRPC health status turn to not serving, so clients
//     and probes stop sending new RPCs here;
//  2. a short pause lets that change propagate;
//  3. the gRPC server stops gracefully, and forcefully after the timeout;
//  4. the expiry sweep stops;
//  5. the admin listener (metrics, probes) stops;
//  6. the database pool closes (deferred, after everything that uses it).
func run(ctx context.Context, stopSignals context.CancelFunc, cfg config, log *slog.Logger) error {
	pool, err := pgdb.Open(ctx, pgdb.Config{
		URL:              cfg.DatabaseURL,
		ApplicationName:  "ledger",
		MaxConns:         int32(cfg.DBMaxConns),
		StatementTimeout: cfg.DBStatementTimeout,
		LockTimeout:      cfg.DBLockTimeout,
		IdleInTxTimeout:  cfg.DBIdleInTxTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	results, err := pgdb.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		return err
	}
	for _, r := range results {
		log.Info("migration applied", "version", r.Source.Version, "path", r.Source.Path)
	}

	// One registry, owned here and injected; nothing uses the global one.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		pgdb.NewPoolCollector(pool),
		postgres.NewCollector(pool, metricsQueryTimeout, log),
	)

	tx := txctx.New(pool)
	store := postgres.NewStore(tx)
	svc := app.NewService(app.Deps{
		Tx:       tx,
		Accounts: store,
		Holds:    store,
		Journal:  store,
		Clock:    platform.SystemClock{},
		Metrics:  metrics.NewBusiness(reg),
		NewID:    uuid.NewV7,
		Logger:   log,
	}, app.Config{DefaultHoldTTL: cfg.HoldDefaultTTL, MaxHoldTTL: cfg.HoldMaxTTL})

	if cfg.SeedFile != "" {
		if err := seed(ctx, cfg.SeedFile, svc, log); err != nil {
			return err
		}
	}

	// Background components run on their own context: they keep going
	// while the gRPC server drains.
	bg := platform.NewGroup(log)
	bg.Go("hold-sweeper", app.NewSweeper(svc, cfg.SweepInterval, cfg.SweepBatch, log).Run)

	var ready platform.Readiness
	admin, err := platform.StartServer("admin", httpserver.NewServer(httpserver.AdminServerConfig(cfg.AdminAddr),
		httpserver.NewAdminHandler(reg, ready.Ready, log), log))
	if err != nil {
		return errors.Join(err, bg.Stop())
	}

	grpcSrv, health := grpcapi.NewServer(grpcapi.NewHandlers(svc, log), grpcapi.NewMetrics(reg), log)
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return errors.Join(fmt.Errorf("grpc listen: %w", err), admin.Stop(ctx, httpserver.AdminShutdownTimeout), bg.Stop())
	}
	grpcDone := make(chan error, 1)
	go func() { grpcDone <- grpcSrv.Serve(lis) }()

	grpcapi.SetServing(health, true)
	ready.Set(true)
	log.Info("ledger started", "grpc_addr", cfg.GRPCAddr, "admin_addr", cfg.AdminAddr)

	var (
		runErr      error
		grpcStopped bool
	)
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "cause", context.Cause(ctx))
	case err := <-grpcDone:
		grpcStopped = true
		runErr = fmt.Errorf("grpc server stopped: %w", err)
	case <-admin.Done():
		runErr = fmt.Errorf("admin server stopped: %w", admin.Err())
	case <-bg.Done():
		runErr = errors.New("a background component stopped")
	}
	// After the first signal the default handler is back, so a second
	// signal kills the process at once.
	stopSignals()

	var errs []error
	ready.Set(false)
	grpcapi.SetServing(health, false)
	log.Info("draining: not serving", "pause", cfg.ShutdownDrainDelay.String())
	pause := time.NewTimer(cfg.ShutdownDrainDelay)
	<-pause.C
	if forced := grpcapi.Stop(grpcSrv, cfg.ShutdownTimeout); forced {
		log.Warn("grpc server did not drain in time; in-flight RPCs were cancelled", "timeout", cfg.ShutdownTimeout.String())
	}
	if !grpcStopped {
		// Serve returns nil after a stop; anything else is a failure.
		if err := <-grpcDone; err != nil {
			errs = append(errs, fmt.Errorf("grpc serve: %w", err))
		}
	}
	log.Info("grpc server stopped")
	if err := bg.Stop(); err != nil {
		errs = append(errs, err)
	}
	if err := admin.Stop(ctx, httpserver.AdminShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(runErr, errors.Join(errs...))
}
