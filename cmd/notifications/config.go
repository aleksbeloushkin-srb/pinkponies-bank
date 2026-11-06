package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"time"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/httpapi"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

// envPrefix scopes the variables of the notifications service.
const envPrefix = "NOTIFICATIONS_"

// knownChannels are the delivery channels this binary can run.
var knownChannels = []string{"log", "webhook"}

// config is the full configuration of the notifications service. Every
// default is a starting point chosen for this reference service, with its
// reason next to it.
type config struct {
	LogLevel slog.Level

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

	KafkaBrokers  []string
	KafkaTopic    string
	KafkaDLQTopic string
	KafkaGroup    string

	ConsumerMaxPollRecords int
	ConsumerMaxAttempts    int
	ConsumerRetryBase      time.Duration
	ConsumerRetryMax       time.Duration
	ConsumerHandleTimeout  time.Duration
	ConsumerCommitTimeout  time.Duration

	Channels            []string
	WebhookURL          string
	WebhookTimeout      time.Duration
	DeliveryMaxAttempts int

	WorkerConcurrency  int
	WorkerIdleInterval time.Duration
	TaskLease          time.Duration
	TaskTimeout        time.Duration
	TaskMaxAttempts    int
	TaskRetryBase      time.Duration
	TaskRetryMax       time.Duration

	CORSOrigins []string
	SinkEnabled bool
	SinkFaults  []string
}

func loadConfig(environ []string) (config, error) {
	l := platform.NewLoader(envPrefix, environ)
	cfg := config{
		LogLevel:  l.Level("LOG_LEVEL", slog.LevelInfo),
		HTTPAddr:  l.String("HTTP_ADDR", ":8081"),
		AdminAddr: l.String("ADMIN_ADDR", ":9102"),
		// The feed is a short read; no long polls here.
		HTTPReadHeaderTimeout: l.Duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		HTTPReadTimeout:       l.Duration("HTTP_READ_TIMEOUT", 10*time.Second),
		HTTPWriteTimeout:      l.Duration("HTTP_WRITE_TIMEOUT", 10*time.Second),
		HTTPIdleTimeout:       l.Duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		ShutdownDrainDelay:    l.Duration("SHUTDOWN_DRAIN_DELAY", 3*time.Second),
		ShutdownTimeout:       l.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),

		DatabaseURL: l.String("DATABASE_URL", ""),
		// Worker slots, the consumer, the API and the scrape.
		DBMaxConns:         l.Int("DB_MAX_CONNS", 10),
		DBStatementTimeout: l.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second),
		DBLockTimeout:      l.Duration("DB_LOCK_TIMEOUT", 2*time.Second),
		DBIdleInTxTimeout:  l.Duration("DB_IDLE_IN_TX_TIMEOUT", 10*time.Second),

		KafkaBrokers:  l.List("KAFKA_BROKERS", ""),
		KafkaTopic:    l.String("KAFKA_TOPIC", eventsv1.TopicTransfers),
		KafkaDLQTopic: l.String("KAFKA_DLQ_TOPIC", eventsv1.TopicTransfersDLQ),
		KafkaGroup:    l.String("KAFKA_GROUP", "notifications"),

		// One poll is handled before the next; small polls keep rebalances
		// waiting for little time.
		ConsumerMaxPollRecords: l.Int("CONSUMER_MAX_POLL_RECORDS", 100),
		// Tries of a poison message before the dead-letter topic.
		ConsumerMaxAttempts: l.Int("CONSUMER_MAX_ATTEMPTS", 5),
		ConsumerRetryBase:   l.Duration("CONSUMER_RETRY_BASE", 200*time.Millisecond),
		ConsumerRetryMax:    l.Duration("CONSUMER_RETRY_MAX", 10*time.Second),
		// One event is one short transaction.
		ConsumerHandleTimeout: l.Duration("CONSUMER_HANDLE_TIMEOUT", 5*time.Second),
		ConsumerCommitTimeout: l.Duration("CONSUMER_COMMIT_TIMEOUT", 5*time.Second),

		Channels:   l.List("CHANNELS", "log,webhook"),
		WebhookURL: l.String("WEBHOOK_URL", ""),
		// A healthy receiver answers fast; a slow one is retried later.
		WebhookTimeout: l.Duration("WEBHOOK_TIMEOUT", 2*time.Second),
		// With the backoff below, a few minutes of retries before a
		// delivery is marked failed.
		DeliveryMaxAttempts: l.Int("DELIVERY_MAX_ATTEMPTS", 8),

		WorkerConcurrency:  l.Int("WORKER_CONCURRENCY", 4),
		WorkerIdleInterval: l.Duration("WORKER_IDLE_INTERVAL", time.Second),
		TaskLease:          l.Duration("TASK_LEASE", 30*time.Second),
		TaskTimeout:        l.Duration("TASK_TIMEOUT", 10*time.Second),
		TaskMaxAttempts:    l.Int("TASK_MAX_ATTEMPTS", 12),
		TaskRetryBase:      l.Duration("TASK_RETRY_BASE", 500*time.Millisecond),
		TaskRetryMax:       l.Duration("TASK_RETRY_MAX", 30*time.Second),

		// Browser origins allowed to read the feed, such as the transfers UI.
		CORSOrigins: l.List("CORS_ORIGINS", ""),
		// The fake webhook receiver, for development only.
		SinkEnabled: l.Bool("SINK_ENABLED", false),
		SinkFaults:  l.List("SINK_FAULTS", ""),
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
		"HTTP_READ_HEADER_TIMEOUT": c.HTTPReadHeaderTimeout, "HTTP_READ_TIMEOUT": c.HTTPReadTimeout,
		"HTTP_WRITE_TIMEOUT": c.HTTPWriteTimeout, "HTTP_IDLE_TIMEOUT": c.HTTPIdleTimeout, "SHUTDOWN_TIMEOUT": c.ShutdownTimeout,
		"DB_STATEMENT_TIMEOUT": c.DBStatementTimeout, "DB_LOCK_TIMEOUT": c.DBLockTimeout, "DB_IDLE_IN_TX_TIMEOUT": c.DBIdleInTxTimeout,
		"CONSUMER_RETRY_BASE": c.ConsumerRetryBase, "CONSUMER_RETRY_MAX": c.ConsumerRetryMax,
		"CONSUMER_HANDLE_TIMEOUT": c.ConsumerHandleTimeout, "CONSUMER_COMMIT_TIMEOUT": c.ConsumerCommitTimeout,
		"WEBHOOK_TIMEOUT": c.WebhookTimeout, "WORKER_IDLE_INTERVAL": c.WorkerIdleInterval, "TASK_LEASE": c.TaskLease,
		"TASK_TIMEOUT": c.TaskTimeout, "TASK_RETRY_BASE": c.TaskRetryBase, "TASK_RETRY_MAX": c.TaskRetryMax,
	})
	ch.Check(c.ShutdownDrainDelay >= 0, "SHUTDOWN_DRAIN_DELAY must not be negative")
	ch.Check(c.DatabaseURL != "", "DATABASE_URL is required")
	ch.Check(len(c.KafkaBrokers) > 0, "KAFKA_BROKERS is required")
	ch.Check(c.KafkaTopic != "" && c.KafkaDLQTopic != "" && c.KafkaGroup != "", "KAFKA_TOPIC, KAFKA_DLQ_TOPIC and KAFKA_GROUP must not be empty")
	ch.Check(c.KafkaTopic != c.KafkaDLQTopic, "KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC, or a poison message loops forever")
	ch.Check(c.AdminAddr != c.HTTPAddr, "ADMIN_ADDR must differ from HTTP_ADDR")
	ch.Check(c.DBMaxConns >= 2 && c.DBMaxConns <= 1000, "DB_MAX_CONNS must be between 2 and 1000")
	ch.Check(c.ConsumerMaxPollRecords >= 1, "CONSUMER_MAX_POLL_RECORDS must be at least 1")
	ch.Check(c.ConsumerMaxAttempts >= 1, "CONSUMER_MAX_ATTEMPTS must be at least 1")
	ch.Check(c.ConsumerRetryBase <= c.ConsumerRetryMax, "CONSUMER_RETRY_BASE must not exceed CONSUMER_RETRY_MAX")
	ch.Check(c.TaskRetryBase <= c.TaskRetryMax, "TASK_RETRY_BASE must not exceed TASK_RETRY_MAX")
	ch.Check(c.DeliveryMaxAttempts >= 1, "DELIVERY_MAX_ATTEMPTS must be at least 1")
	ch.Check(c.WorkerConcurrency >= 1, "WORKER_CONCURRENCY must be at least 1")
	ch.Check(len(c.Channels) > 0, "CHANNELS must name at least one channel")
	for i, name := range c.Channels {
		ch.Check(slices.Contains(knownChannels, name), "CHANNELS: unknown channel %q (want %v)", name, knownChannels)
		// A repeated channel would make two deliveries of every
		// notification on it, and the second breaks the deliveries key.
		ch.Check(!slices.Contains(c.Channels[:i], name), "CHANNELS: %q is listed twice", name)
	}
	if slices.Contains(c.Channels, "webhook") {
		ch.Check(validHTTPURL(c.WebhookURL), "WEBHOOK_URL must be an http(s) URL when the webhook channel is on")
	}
	for _, o := range c.CORSOrigins {
		ch.Check(validOrigin(o), "CORS_ORIGINS: %q is not an origin like http://localhost:8080", o)
	}
	ch.Check(c.SinkEnabled || len(c.SinkFaults) == 0, "SINK_FAULTS needs SINK_ENABLED=true")
	if err := httpapi.CheckSinkFaults(c.SinkFaults); err != nil {
		ch.Check(false, "SINK_FAULTS: %v", err)
	}

	// Relations between settings: each one guards a failure mode.
	ch.DBTimeouts(c.DBStatementTimeout, c.DBLockTimeout, c.DBIdleInTxTimeout)
	ch.Check(c.WebhookTimeout+5*time.Second <= c.TaskTimeout,
		"TASK_TIMEOUT must exceed WEBHOOK_TIMEOUT by at least 5s: a delivery is one call and two statements")
	ch.Check(c.TaskTimeout+10*time.Second <= c.TaskLease,
		"TASK_TIMEOUT must be at least 10s shorter than TASK_LEASE, or a slow delivery is claimed twice")
	ch.Check(c.TaskMaxAttempts > c.DeliveryMaxAttempts,
		"TASK_MAX_ATTEMPTS must exceed DELIVERY_MAX_ATTEMPTS, or a failing delivery goes dead instead of failed")
	ch.Check(c.WorkerConcurrency+3 <= c.DBMaxConns,
		"WORKER_CONCURRENCY must be at most DB_MAX_CONNS-3, leaving connections for the consumer, the API and the scrape")
	return ch.Err()
}

func validHTTPURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func validOrigin(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.Path == "" && fmt.Sprint(u) == s
}
