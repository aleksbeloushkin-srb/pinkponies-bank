// Command fakerisk runs the simulated screening provider (see package
// internal/fakerisk) on FAKERISK_ADDR. It is a stand-in for a third party
// in development and tests, not a service of the bank.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/fakerisk"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform"
)

const envPrefix = "FAKERISK_"

type config struct {
	Addr string
	Risk fakerisk.Config
}

func loadConfig(environ []string) (config, error) {
	l := platform.NewLoader(envPrefix, environ)
	cfg := config{
		Addr: l.String("ADDR", ":8090"),
		Risk: fakerisk.Config{
			Latency:    l.Duration("LATENCY", 0),
			RetryAfter: l.Duration("RETRY_AFTER", time.Second),
			// Long enough to watch "review" in the UI, short enough for the
			// end-to-end script.
			ReviewDelay: l.Duration("REVIEW_DELAY", 3*time.Second),
		},
	}
	faults, err := fakerisk.ParseFaults(l.String("FAULTS", ""))
	cfg.Risk.Faults = faults
	if err := errors.Join(l.Err(), err); err != nil {
		return config{}, err
	}
	ch := platform.NewChecks(envPrefix)
	ch.Check(cfg.Risk.Latency >= 0 && cfg.Risk.RetryAfter >= 0 && cfg.Risk.ReviewDelay >= 0, "LATENCY, RETRY_AFTER and REVIEW_DELAY must not be negative")
	return cfg, ch.Err()
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		addr := os.Getenv(envPrefix + "ADDR")
		if addr == "" {
			addr = ":8090"
		}
		os.Exit(platform.Healthcheck(addr))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := loadConfig(os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, "fakerisk: config:", err)
		os.Exit(1)
	}
	log := platform.NewLogger(os.Stdout, slog.LevelInfo)
	if err := run(ctx, stop, cfg, log); err != nil {
		log.Error("fakerisk stopped with an error", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stopSignals context.CancelFunc, cfg config, log *slog.Logger) error {
	risk := fakerisk.New(cfg.Risk, platform.SystemClock{}, log)
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           risk.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Hanging requests end when shutdown starts, or Shutdown would wait for
	// them until its deadline.
	srv.RegisterOnShutdown(risk.Close)
	s, err := platform.StartServer("fakerisk", srv)
	if err != nil {
		return err
	}
	log.Info("fakerisk listening", "addr", cfg.Addr, "faults", cfg.Risk.Faults, "review_delay", cfg.Risk.ReviewDelay.String())
	select {
	case <-ctx.Done():
	case <-s.Done():
		return s.Err()
	}
	stopSignals()
	return s.Stop(ctx, 10*time.Second)
}
