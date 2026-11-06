package main

import (
	"log/slog"
	"time"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// envPrefix scopes the variables of the ledger. Any unknown variable with
// the prefix stops the start.
const envPrefix = "LEDGER_"

// config is the full configuration of the ledger.
//
// Every default is a value chosen for this reference service, with the
// reason next to it: starting points to tune with metrics, not
// recommendations for any particular system.
type config struct {
	LogLevel  slog.Level
	GRPCAddr  string
	AdminAddr string

	DatabaseURL        string
	DBMaxConns         int
	DBStatementTimeout time.Duration
	DBLockTimeout      time.Duration
	DBIdleInTxTimeout  time.Duration

	HoldDefaultTTL time.Duration
	HoldMaxTTL     time.Duration
	SweepInterval  time.Duration
	SweepBatch     int

	ShutdownDrainDelay time.Duration
	ShutdownTimeout    time.Duration

	SeedFile string
}

func loadConfig(environ []string) (config, error) {
	l := platform.NewLoader(envPrefix, environ)
	cfg := config{
		LogLevel: l.Level("LOG_LEVEL", slog.LevelInfo),
		GRPCAddr: l.String("GRPC_ADDR", ":50051"),
		// Metrics and probes, for the cluster network only.
		AdminAddr: l.String("ADMIN_ADDR", ":9101"),

		DatabaseURL: l.String("DATABASE_URL", ""),
		// Every RPC is one short transaction; the sweep and the scrape need
		// one more each.
		DBMaxConns: l.Int("DB_MAX_CONNS", 16),
		// Ledger statements touch a handful of rows by primary key.
		DBStatementTimeout: l.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second),
		// Waiters for a hot account fail fast with Unavailable and come back
		// through the caller's retry with the same key.
		DBLockTimeout: l.Duration("DB_LOCK_TIMEOUT", 2*time.Second),
		// No RPC keeps a transaction open while it waits for anything else.
		DBIdleInTxTimeout: l.Duration("DB_IDLE_IN_TX_TIMEOUT", 10*time.Second),

		// Longer than any healthy transfer, short enough that a forgotten
		// hold does not block a client's money for long.
		HoldDefaultTTL: l.Duration("HOLD_DEFAULT_TTL", 15*time.Minute),
		HoldMaxTTL:     l.Duration("HOLD_MAX_TTL", 24*time.Hour),
		// Expired holds give money back within seconds of their expiry.
		SweepInterval: l.Duration("SWEEP_INTERVAL", 5*time.Second),
		SweepBatch:    l.Int("SWEEP_BATCH", 100),

		// Time for clients' health checks to see NOT_SERVING.
		ShutdownDrainDelay: l.Duration("SHUTDOWN_DRAIN_DELAY", 2*time.Second),
		// In-flight RPCs are short; after this they are cancelled.
		ShutdownTimeout: l.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),

		// Optional JSON file of accounts to open at start, idempotently.
		SeedFile: l.String("SEED_FILE", ""),
	}
	if err := l.Err(); err != nil {
		return config{}, err
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	ch := platform.NewChecks(envPrefix)
	ch.Positive(map[string]time.Duration{
		"DB_STATEMENT_TIMEOUT": c.DBStatementTimeout, "DB_LOCK_TIMEOUT": c.DBLockTimeout,
		"DB_IDLE_IN_TX_TIMEOUT": c.DBIdleInTxTimeout, "HOLD_DEFAULT_TTL": c.HoldDefaultTTL,
		"HOLD_MAX_TTL": c.HoldMaxTTL, "SWEEP_INTERVAL": c.SweepInterval, "SHUTDOWN_TIMEOUT": c.ShutdownTimeout,
	})
	ch.Check(c.ShutdownDrainDelay >= 0, "SHUTDOWN_DRAIN_DELAY must not be negative")
	ch.Check(c.DatabaseURL != "", "DATABASE_URL is required")
	ch.Check(c.GRPCAddr != "" && c.AdminAddr != "", "GRPC_ADDR and ADMIN_ADDR are required")
	ch.Check(c.GRPCAddr != c.AdminAddr, "ADMIN_ADDR must differ from GRPC_ADDR")
	ch.Check(c.DBMaxConns >= 2 && c.DBMaxConns <= 1000, "DB_MAX_CONNS must be between 2 and 1000")
	ch.Check(c.SweepBatch >= 1 && c.SweepBatch <= 10_000, "SWEEP_BATCH must be between 1 and 10000")
	ch.Check(c.HoldDefaultTTL <= c.HoldMaxTTL, "HOLD_DEFAULT_TTL must not exceed HOLD_MAX_TTL")
	ch.DBTimeouts(c.DBStatementTimeout, c.DBLockTimeout, c.DBIdleInTxTimeout)
	return ch.Err()
}
