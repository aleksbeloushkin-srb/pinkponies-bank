//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	ledgermigrations "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/migrations"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
	transfersmigrations "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/migrations"
)

// Pools of the service databases, shared by all tests. Each test resets the
// tables it uses first, so the tests of this package must not run in
// parallel.
var ledgerDB, transfersDB *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()

	host, err := ctr.Host(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres host:", err)
		return 1
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres port:", err)
		return 1
	}
	dsn := func(db string) string {
		return "postgres://postgres:postgres@" + net.JoinHostPort(host, port.Port()) + "/" + db + "?sslmode=disable"
	}
	admin, err := connect(ctx, dsn, "postgres", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, db := range []string{"ledger", "transfers"} {
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
			fmt.Fprintln(os.Stderr, "create database:", err)
			return 1
		}
	}
	admin.Close()

	if ledgerDB, err = connect(ctx, dsn, "ledger", ledgermigrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer ledgerDB.Close()
	if transfersDB, err = connect(ctx, dsn, "transfers", transfersmigrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer transfersDB.Close()
	return m.Run()
}

// connect opens a pool on one database and applies its migrations.
func connect(ctx context.Context, dsn func(string) string, db string, migrations fs.FS) (*pgxpool.Pool, error) {
	pool, err := pgdb.Open(ctx, pgdb.Config{
		URL:              dsn(db),
		ApplicationName:  "integration-" + db,
		MaxConns:         24,
		StatementTimeout: 10 * time.Second,
		LockTimeout:      5 * time.Second,
		IdleInTxTimeout:  30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: open: %w", db, err)
	}
	if migrations != nil {
		if _, err := pgdb.Migrate(ctx, pool, migrations); err != nil {
			pool.Close()
			return nil, fmt.Errorf("%s: migrate: %w", db, err)
		}
	}
	return pool, nil
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// clock is a settable clock shared by a service and its background loops.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}
