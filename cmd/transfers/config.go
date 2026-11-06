package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"time"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/kafkapub"
)

// envPrefix scopes the variables of the transfers service.
const envPrefix = "TRANSFERS_"

// roles selects which parts of the binary run in this process.
type roles struct {
	API    bool // REST API, long polling, web UI
	Worker bool // saga steps from the task queue
	Relay  bool // outbox to Kafka
}

// config is the full configuration of the transfers service.
//
// Every default is a value chosen for this reference service, with the
// reason next to it: starting points to tune with metrics, not
// recommendations for any particular system.
type config struct {
	LogLevel slog.Level
	Roles    roles

	HTTPAddr              string
	AdminAddr             string
	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	ShutdownDrainDelay    time.Duration
	ShutdownTimeout       time.Duration

	DatabaseURL        string
	DBMaxConns         int
	DBStatementTimeout time.Duration
	DBLockTimeout      time.Duration
	DBIdleInTxTimeout  time.Duration

	LedgerAddr    string
	LedgerTimeout time.Duration
	RiskURL       string
	RiskTimeout   time.Duration
	// RiskMaxRetryAfter caps the wait a provider's 429 may ask for.
	RiskMaxRetryAfter time.Duration

	WorkerConcurrency  int
	WorkerIdleInterval time.Duration
	TaskLease          time.Duration
	TaskTimeout        time.Duration
	TaskMaxAttempts    int
	TaskRetryBase      time.Duration
	TaskRetryMax       time.Duration

	HoldTTL             time.Duration
	ScreenMaxFailures   int
	ScreenReviewTimeout time.Duration
	ScreenPollMin       time.Duration
	ScreenPollMax       time.Duration

	WaitTimeout       time.Duration
	WaitReadTimeout   time.Duration
	WaitMaxWaiters    int
	WaitBatchInterval time.Duration

	KafkaBrokers   []string
	KafkaTopic     string
	RelayInterval  time.Duration
	RelayBatch     int
	RelayLease     time.Duration
	PublishTimeout time.Duration

	UINotificationsURL string
}

func loadConfig(environ []string) (config, error) {
	l := platform.NewLoader(envPrefix, environ)
	cfg := config{
		LogLevel: l.Level("LOG_LEVEL", slog.LevelInfo),
		// One process runs everything locally; production scales the roles
		// independently.
		Roles: parseRoles(l, "api,worker,relay"),

		HTTPAddr: l.String("HTTP_ADDR", ":8080"),
		// Metrics and probes, for the cluster network only.
		AdminAddr: l.String("ADMIN_ADDR", ":9100"),
		// Slow-header clients cannot pin connections for long.
		HTTPReadHeaderTimeout: l.Duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		// Request bodies are a few hundred bytes.
		HTTPReadTimeout: l.Duration("HTTP_READ_TIMEOUT", 10*time.Second),
		// Twice the long-poll soft timeout: the wait, its two bounded reads
		// and the write fit with room to spare (see the check below).
		HTTPWriteTimeout: l.Duration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		HTTPIdleTimeout:  l.Duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		// Time for a load balancer to notice the failing readiness probe.
		ShutdownDrainDelay: l.Duration("SHUTDOWN_DRAIN_DELAY", 3*time.Second),
		// In-flight requests; long polls are released at once.
		ShutdownTimeout: l.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),

		DatabaseURL: l.String("DATABASE_URL", ""),
		// Worker slots, API requests, the batcher, with room to spare.
		DBMaxConns:         l.Int("DB_MAX_CONNS", 12),
		DBStatementTimeout: l.Duration("DB_STATEMENT_TIMEOUT", 10*time.Second),
		// Lock waiters fail fast and come back through task retries.
		DBLockTimeout: l.Duration("DB_LOCK_TIMEOUT", 5*time.Second),
		// No transaction is held across a remote call, so a long idle
		// transaction is always a bug.
		DBIdleInTxTimeout: l.Duration("DB_IDLE_IN_TX_TIMEOUT", 15*time.Second),

		// A gRPC target, for example dns:///ledger:50051.
		LedgerAddr: l.String("LEDGER_ADDR", ""),
		// Ledger RPCs are a few short transactions; a slower answer is a
		// fault to retry, not a reason to wait.
		LedgerTimeout: l.Duration("LEDGER_TIMEOUT", 3*time.Second),
		RiskURL:       l.String("RISK_URL", ""),
		// A healthy provider answers in well under a second.
		RiskTimeout: l.Duration("RISK_TIMEOUT", 2*time.Second),
		// As long as the longest review poll: a 429 gets no more say in how
		// long a step waits than a review does.
		RiskMaxRetryAfter: l.Duration("RISK_MAX_RETRY_AFTER", 30*time.Second),

		WorkerConcurrency: l.Int("WORKER_CONCURRENCY", 4),
		// Commit hooks wake the local worker for new tasks; delayed retries,
		// review polls and tasks of other instances wait for this poll. As
		// long as the first retry delay (TASK_RETRY_BASE): a retry runs at
		// most that much later than its backoff asked.
		WorkerIdleInterval: l.Duration("WORKER_IDLE_INTERVAL", 500*time.Millisecond),
		// Three task timeouts: a live worker never loses its lease, and a
		// crashed worker's task waits less than a minute to run again.
		TaskLease: l.Duration("TASK_LEASE", 45*time.Second),
		// One remote call per step plus two short transactions.
		TaskTimeout: l.Duration("TASK_TIMEOUT", 15*time.Second),
		// With the backoff below, several minutes of retries ride out a short
		// ledger outage before a step goes dead for an operator.
		TaskMaxAttempts: l.Int("TASK_MAX_ATTEMPTS", 15),
		TaskRetryBase:   l.Duration("TASK_RETRY_BASE", 500*time.Millisecond),
		TaskRetryMax:    l.Duration("TASK_RETRY_MAX", 30*time.Second),

		// Bounds the money a stuck transfer blocks. It must outlast the worst
		// case of the hold, screen and capture steps (holdBudget, about 22
		// minutes with these defaults).
		HoldTTL: l.Duration("HOLD_TTL", 30*time.Minute),
		// Failed calls with the same key before the hold is released.
		ScreenMaxFailures: l.Int("SCREEN_MAX_FAILURES", 6),
		// A review longer than this cancels the transfer.
		ScreenReviewTimeout: l.Duration("SCREEN_REVIEW_TIMEOUT", 5*time.Minute),
		// Bounds of the provider's Retry-After for a review.
		ScreenPollMin: l.Duration("SCREEN_POLL_MIN", time.Second),
		ScreenPollMax: l.Duration("SCREEN_POLL_MAX", 30*time.Second),

		// Well under the 30 to 60 second idle timeouts common in proxies.
		WaitTimeout: l.Duration("WAIT_TIMEOUT", 15*time.Second),
		// Each of the two reads of a long poll, and the batch query.
		WaitReadTimeout: l.Duration("WAIT_READ_TIMEOUT", 2*time.Second),
		// Each waiter costs a goroutine and a socket.
		WaitMaxWaiters: l.Int("WAIT_MAX_WAITERS", 2000),
		// Five queries a second per instance, whatever the number of waiters.
		WaitBatchInterval: l.Duration("WAIT_BATCH_INTERVAL", 200*time.Millisecond),

		// Required for the relay role, for example redpanda:9092.
		KafkaBrokers: l.List("KAFKA_BROKERS", ""),
		KafkaTopic:   l.String("KAFKA_TOPIC", eventsv1.TopicTransfers),
		// Commit hooks wake the local relay; this is the fallback cadence.
		RelayInterval: l.Duration("RELAY_INTERVAL", 500*time.Millisecond),
		RelayBatch:    l.Int("RELAY_BATCH", 50),
		// Claimed events stay hidden from other relays this long; a delivery
		// starts only if it can finish inside the lease (see the check below).
		RelayLease: l.Duration("RELAY_LEASE", time.Minute),
		// A broker slower than this to acknowledge counts as failed.
		PublishTimeout: l.Duration("PUBLISH_TIMEOUT", 5*time.Second),

		// The notifications API as the browser sees it; empty hides the feed.
		UINotificationsURL: l.String("UI_NOTIFICATIONS_URL", ""),
	}
	if err := l.Err(); err != nil {
		return config{}, err
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func parseRoles(l *platform.Loader, def string) roles {
	var r roles
	for _, role := range l.List("ROLES", def) {
		switch role {
		case "api":
			r.API = true
		case "worker":
			r.Worker = true
		case "relay":
			r.Relay = true
		default:
			l.Fail("ROLES", fmt.Errorf("unknown role %q (want api, worker, relay)", role))
		}
	}
	return r
}

func (c config) validate() error {
	ch := platform.NewChecks(envPrefix)
	ch.Check(c.Roles != (roles{}), "ROLES must name at least one role")
	ch.Positive(map[string]time.Duration{
		"HTTP_READ_HEADER_TIMEOUT": c.HTTPReadHeaderTimeout, "HTTP_READ_TIMEOUT": c.HTTPReadTimeout,
		"HTTP_WRITE_TIMEOUT": c.HTTPWriteTimeout, "HTTP_IDLE_TIMEOUT": c.HTTPIdleTimeout,
		"SHUTDOWN_TIMEOUT": c.ShutdownTimeout, "DB_STATEMENT_TIMEOUT": c.DBStatementTimeout,
		"DB_LOCK_TIMEOUT": c.DBLockTimeout, "DB_IDLE_IN_TX_TIMEOUT": c.DBIdleInTxTimeout,
		"LEDGER_TIMEOUT": c.LedgerTimeout, "RISK_TIMEOUT": c.RiskTimeout, "RISK_MAX_RETRY_AFTER": c.RiskMaxRetryAfter,
		"WORKER_IDLE_INTERVAL": c.WorkerIdleInterval, "TASK_LEASE": c.TaskLease, "TASK_TIMEOUT": c.TaskTimeout,
		"TASK_RETRY_BASE": c.TaskRetryBase, "TASK_RETRY_MAX": c.TaskRetryMax, "HOLD_TTL": c.HoldTTL,
		"SCREEN_REVIEW_TIMEOUT": c.ScreenReviewTimeout, "SCREEN_POLL_MIN": c.ScreenPollMin,
		"SCREEN_POLL_MAX": c.ScreenPollMax, "WAIT_TIMEOUT": c.WaitTimeout, "WAIT_READ_TIMEOUT": c.WaitReadTimeout,
		"WAIT_BATCH_INTERVAL": c.WaitBatchInterval, "RELAY_INTERVAL": c.RelayInterval, "RELAY_LEASE": c.RelayLease,
		"PUBLISH_TIMEOUT": c.PublishTimeout,
	})
	ch.Check(c.ShutdownDrainDelay >= 0, "SHUTDOWN_DRAIN_DELAY must not be negative")
	ch.Check(c.DatabaseURL != "", "DATABASE_URL is required")
	ch.Check(c.LedgerAddr != "", "LEDGER_ADDR is required: the api lists accounts and the worker places holds")
	ch.Check(c.DBMaxConns >= 2 && c.DBMaxConns <= 1000, "DB_MAX_CONNS must be between 2 and 1000")
	ch.Check(c.WorkerConcurrency >= 1, "WORKER_CONCURRENCY must be at least 1")
	ch.Check(c.TaskMaxAttempts >= 1, "TASK_MAX_ATTEMPTS must be at least 1")
	ch.Check(c.ScreenMaxFailures >= 1, "SCREEN_MAX_FAILURES must be at least 1")
	ch.Check(c.WaitMaxWaiters >= 1, "WAIT_MAX_WAITERS must be at least 1")
	ch.Check(c.RelayBatch >= 1, "RELAY_BATCH must be at least 1")
	ch.Check(c.TaskRetryBase <= c.TaskRetryMax, "TASK_RETRY_BASE must not exceed TASK_RETRY_MAX")
	ch.Check(c.ScreenPollMin <= c.ScreenPollMax, "SCREEN_POLL_MIN must not exceed SCREEN_POLL_MAX")
	ch.Check(!c.Roles.API || c.AdminAddr != c.HTTPAddr, "ADMIN_ADDR must differ from HTTP_ADDR")

	// Relations between settings: each one guards a failure mode.
	ch.Check(c.HTTPWriteTimeout >= c.WaitTimeout+2*c.WaitReadTimeout+5*time.Second,
		"HTTP_WRITE_TIMEOUT must exceed WAIT_TIMEOUT + 2 x WAIT_READ_TIMEOUT by at least 5s, or long polls are cut off")
	ch.Check(c.TaskTimeout+10*time.Second <= c.TaskLease,
		"TASK_TIMEOUT must be at least 10s shorter than TASK_LEASE, or a slow step is claimed twice")
	ch.Check(max(c.LedgerTimeout, c.RiskTimeout)+5*time.Second <= c.TaskTimeout,
		"TASK_TIMEOUT must exceed LEDGER_TIMEOUT and RISK_TIMEOUT by at least 5s: a step is one call and two transactions")
	ch.Check(c.TaskMaxAttempts > c.ScreenMaxFailures,
		"TASK_MAX_ATTEMPTS must exceed SCREEN_MAX_FAILURES, or a failing screening goes dead instead of releasing its hold")
	ch.Check(c.RiskMaxRetryAfter < c.ScreenReviewTimeout,
		"RISK_MAX_RETRY_AFTER must be shorter than SCREEN_REVIEW_TIMEOUT, or one 429 uses up the screening's whole time")
	ch.Check(c.HoldTTL >= c.holdBudget(),
		"HOLD_TTL must be at least %s with these settings (the worst case of the hold, screen and capture steps), or an approved transfer can find its hold expired",
		c.holdBudget().Round(time.Second))
	ch.Check(c.WorkerConcurrency+3 <= c.DBMaxConns,
		"WORKER_CONCURRENCY must be at most DB_MAX_CONNS-3, leaving connections for the API and the batcher")
	ch.DBTimeouts(c.DBStatementTimeout, c.DBLockTimeout, c.DBIdleInTxTimeout)
	ch.Check(c.RelayLease >= 2*kafkapub.DeliveryBound(c.PublishTimeout),
		"RELAY_LEASE must be at least %s, twice the longest delivery (PUBLISH_TIMEOUT and the producer's request limits), or the relay publishes almost nothing per batch",
		2*kafkapub.DeliveryBound(c.PublishTimeout))
	if c.Roles.Relay {
		ch.Check(len(c.KafkaBrokers) > 0, "KAFKA_BROKERS is required when the relay role is on")
		ch.Check(c.KafkaTopic != "", "KAFKA_TOPIC must not be empty")
	}
	if c.Roles.Worker {
		ch.Check(validHTTPURL(c.RiskURL), "RISK_URL must be an http(s) URL when the worker role is on")
	}
	if c.UINotificationsURL != "" {
		ch.Check(validHTTPURL(c.UINotificationsURL), "UI_NOTIFICATIONS_URL must be an http(s) URL")
	}
	return ch.Err()
}

// stepBudget is the worst case of a ledger step (hold or capture) that
// still succeeds: all TASK_MAX_ATTEMPTS runs use their whole TASK_TIMEOUT,
// and between two runs the task waits the longest backoff and one idle
// poll.
func (c config) stepBudget() time.Duration {
	b := taskqueue.Backoff{Base: c.TaskRetryBase, Max: c.TaskRetryMax}
	d := c.TaskTimeout
	for attempt := 1; attempt < c.TaskMaxAttempts; attempt++ {
		d += b.Ceiling(attempt) + c.WorkerIdleInterval + c.TaskTimeout
	}
	return d
}

// holdBudget is how long a hold must live for an approved transfer to still
// find it at capture. The time to live starts when the ledger places the
// hold, so it covers the hold step's retries after a lost answer, the
// screen step (SCREEN_REVIEW_TIMEOUT from its first run bounds it, plus
// the run that approves) and the capture step's retries, with one idle poll
// between steps.
func (c config) holdBudget() time.Duration {
	return 2*c.stepBudget() + c.ScreenReviewTimeout + c.TaskTimeout + 2*c.WorkerIdleInterval
}

func validHTTPURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
