// Package pgdb opens a PostgreSQL pool with server-side safety limits,
// applies embedded goose migrations under a lock, exports the pool's
// statistics to Prometheus and classifies errors as transient or not.
//
// Every service in this repository owns its database and calls Open and
// Migrate from its composition root.
package pgdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Config tunes the connection pool and the server-side safety limits.
type Config struct {
	URL string
	// ApplicationName shows up in pg_stat_activity and in server logs.
	ApplicationName string
	MaxConns        int32
	// StatementTimeout aborts any single statement that runs longer.
	StatementTimeout time.Duration
	// LockTimeout aborts a statement that waits longer for a row lock.
	LockTimeout time.Duration
	// IdleInTxTimeout closes a session that sits idle inside an open
	// transaction, so a stuck client cannot hold locks forever.
	IdleInTxTimeout time.Duration
}

// pingTimeout bounds the start-up connectivity check.
const pingTimeout = 5 * time.Second

// Open creates the pool and checks connectivity. pgxpool connects lazily, so
// without the ping a wrong URL would only surface on the first request.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse url: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	params := pc.ConnConfig.RuntimeParams
	params["application_name"] = cfg.ApplicationName
	params["statement_timeout"] = millis(cfg.StatementTimeout)
	params["lock_timeout"] = millis(cfg.LockTimeout)
	params["idle_in_transaction_session_timeout"] = millis(cfg.IdleInTxTimeout)

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

func millis(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// Migrate applies pending migrations from fsys. A session-level advisory
// lock serializes concurrent starts of several instances.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]*goose.MigrationResult, error) {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("postgres: migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return nil, fmt.Errorf("postgres: migrations: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: migrate up: %w", err)
	}
	return results, nil
}

// SQLSTATE codes that a retry of the whole transaction can fix.
const (
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
	codeLockNotAvailable     = "55P03" // lock_timeout
	codeQueryCanceled        = "57014" // statement_timeout
)

// IsTransient reports whether err is a database failure that the same
// request may not hit again: a lost or refused connection, a deadline, or a
// conflict with a concurrent transaction (serialization failure, deadlock,
// lock or statement timeout). Constraint violations and syntax errors are
// not transient: retrying them gives the same answer.
func IsTransient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case codeSerializationFailure, codeDeadlockDetected, codeLockNotAvailable, codeQueryCanceled:
			return true
		}
		// Class 08 is "connection exception", class 53 "insufficient
		// resources" (too many connections, out of memory or disk).
		return len(pgErr.Code) == 5 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "53")
	}
	var connErr *pgconn.ConnectError
	return errors.As(err, &connErr) ||
		pgconn.Timeout(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		pgconn.SafeToRetry(err)
}

// IsDataError reports whether the database refused the data itself: class
// 22 (data exception, such as a value out of range or invalid text) or
// class 23 (integrity constraint violation). The same data is refused on
// every retry.
func IsDataError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && len(pgErr.Code) == 5 && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23")
}

// IsUniqueViolation reports whether err is a unique constraint violation,
// optionally of the named constraint.
func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}
