// Command notifications is the Pinkponies Bank notifications service: it
// consumes transfers.events as a Kafka consumer group, turns each event
// into notifications exactly once, delivers them on fake channels (a log
// and a webhook), and serves an account's feed over REST.
//
// main is the composition root: configuration, adapters, use cases,
// components, and the order in which they stop. Read it top to bottom.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/channels"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/consumer"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/httpapi"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/metrics"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/migrations"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/kafka"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/pgdb"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// metricsQueryTimeout bounds the queries behind the gauges.
const metricsQueryTimeout = 2 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv(envPrefix + "ADMIN_ADDR")
		if addr == "" {
			addr = ":9102"
		}
		os.Exit(platform.Healthcheck(addr))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig(os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, "notifications: config:", err)
		os.Exit(1)
	}
	log := platform.NewLogger(os.Stdout, cfg.LogLevel)
	if err := run(ctx, stop, cfg, log); err != nil {
		log.Error("notifications stopped with an error", "err", err)
		os.Exit(1)
	}
	log.Info("notifications stopped")
}

// run builds the service, serves until ctx is cancelled or a component
// fails, and then shuts down in order:
//
//  1. readiness turns false;
//  2. a short pause lets the readiness change propagate;
//  3. the API drains (the fake sink releases hanging requests);
//  4. the consumer finishes the record in hand and commits what it
//     handled; the delivery worker finishes its running deliveries;
//  5. the Kafka client leaves the group and closes, so its partitions
//     move to another member at once instead of after a session timeout;
//  6. the admin listener stops, then the pool closes.
func run(ctx context.Context, stopSignals context.CancelFunc, cfg config, log *slog.Logger) error {
	pool, err := pgdb.Open(ctx, pgdb.Config{
		URL:              cfg.DatabaseURL,
		ApplicationName:  "notifications",
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

	cl, err := kafka.NewClient(kafka.Config{Brokers: cfg.KafkaBrokers, ClientID: "notifications"}, log,
		consumer.Options(cfg.KafkaGroup, cfg.KafkaTopic)...)
	if err != nil {
		return err
	}
	clientClosed := false
	closeClient := func() {
		if !clientClosed {
			clientClosed = true
			cl.Close()
		}
	}
	defer closeClient()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		pgdb.NewPoolCollector(pool),
		taskqueue.NewCollector(pool, app.TaskKinds, metricsQueryTimeout, log),
		kafka.NewCollector(kadm.NewClient(cl), cfg.KafkaGroup, []string{cfg.KafkaTopic}, []string{cfg.KafkaDLQTopic}, metricsQueryTimeout, log),
	)

	chans, err := newChannels(cfg, log)
	if err != nil {
		return err
	}
	tx := txctx.New(pool)
	tasks := taskqueue.NewPostgres(tx)
	svc := app.NewService(app.Deps{
		Tx:       tx,
		Store:    postgres.NewStore(tx),
		Tasks:    tasks,
		Channels: chans,
		Clock:    platform.SystemClock{},
		Metrics:  metrics.NewBusiness(reg, cfg.Channels),
		NewID:    uuid.NewV7,
		Logger:   log,
	}, app.Config{DeliveryMaxAttempts: cfg.DeliveryMaxAttempts})

	bg := platform.NewGroup(log)
	bg.Go("consumer", consumer.New(cl, consumer.NewDeadLetterTopic(cl, cfg.KafkaDLQTopic), svc, consumer.NewMetrics(reg), consumer.Config{
		MaxPollRecords: cfg.ConsumerMaxPollRecords,
		MaxAttempts:    cfg.ConsumerMaxAttempts,
		Backoff:        taskqueue.Backoff{Base: cfg.ConsumerRetryBase, Max: cfg.ConsumerRetryMax},
		HandleTimeout:  cfg.ConsumerHandleTimeout,
		CommitTimeout:  cfg.ConsumerCommitTimeout,
	}, log).Run)
	worker := taskqueue.NewWorker(tasks, svc, platform.SystemClock{}, tasks.Ready(), taskqueue.NewMetrics(reg, app.TaskKinds), log, taskqueue.Config{
		Concurrency:  cfg.WorkerConcurrency,
		IdleInterval: cfg.WorkerIdleInterval,
		Lease:        cfg.TaskLease,
		TaskTimeout:  cfg.TaskTimeout,
		MaxAttempts:  cfg.TaskMaxAttempts,
		Backoff:      taskqueue.Backoff{Base: cfg.TaskRetryBase, Max: cfg.TaskRetryMax},
	})
	bg.Go("delivery-worker", worker.Run)

	var ready platform.Readiness
	admin, err := platform.StartServer("admin", httpserver.NewServer(httpserver.AdminServerConfig(cfg.AdminAddr),
		httpserver.NewAdminHandler(reg, ready.Ready, log), log))
	if err != nil {
		return errors.Join(err, bg.Stop())
	}
	var sink *httpapi.Sink
	if cfg.SinkEnabled {
		if sink, err = httpapi.NewSink(cfg.SinkFaults); err != nil {
			return errors.Join(err, admin.Stop(ctx, httpserver.AdminShutdownTimeout), bg.Stop())
		}
	}
	srv := httpserver.NewServer(httpserver.ServerConfig{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}, httpapi.NewHandler(httpapi.Deps{
		Lister:      svc,
		CORSOrigins: cfg.CORSOrigins,
		Sink:        sink,
		Metrics:     httpserver.NewMetrics(reg),
		Logger:      log,
	}), log)
	if sink != nil {
		srv.RegisterOnShutdown(sink.Close)
	}
	api, err := platform.StartServer("api", srv)
	if err != nil {
		return errors.Join(err, admin.Stop(ctx, httpserver.AdminShutdownTimeout), bg.Stop())
	}
	ready.Set(true)
	log.Info("notifications started", "addr", cfg.HTTPAddr, "admin_addr", cfg.AdminAddr,
		"topic", cfg.KafkaTopic, "group", cfg.KafkaGroup, "channels", cfg.Channels, "sink", cfg.SinkEnabled)

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "cause", context.Cause(ctx))
	case <-api.Done():
		runErr = fmt.Errorf("api server stopped: %w", api.Err())
	case <-admin.Done():
		runErr = fmt.Errorf("admin server stopped: %w", admin.Err())
	case <-bg.Done():
		runErr = errors.New("a background component stopped")
	}
	stopSignals()

	var errs []error
	ready.Set(false)
	worker.StopClaiming()
	log.Info("draining: readiness is off", "pause", cfg.ShutdownDrainDelay.String())
	pause := time.NewTimer(cfg.ShutdownDrainDelay)
	<-pause.C
	if err := api.Stop(ctx, cfg.ShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	if err := bg.Stop(); err != nil {
		errs = append(errs, err)
	}
	closeClient()
	log.Info("consumer and worker stopped, left the consumer group")
	if err := admin.Stop(ctx, httpserver.AdminShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(runErr, errors.Join(errs...))
}

func newChannels(cfg config, log *slog.Logger) ([]app.Channel, error) {
	var out []app.Channel
	for _, name := range cfg.Channels {
		switch name {
		case "log":
			out = append(out, channels.NewLog(log))
		case "webhook":
			wh, err := channels.NewWebhook(cfg.WebhookURL, platform.NewHTTPClient(), cfg.WebhookTimeout)
			if err != nil {
				return nil, err
			}
			out = append(out, wh)
		}
	}
	return out, nil
}
