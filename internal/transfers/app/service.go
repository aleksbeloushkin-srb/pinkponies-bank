// Package app implements the transfers use cases: accept a transfer,
// read it, wait for its changes, list accounts, and run the saga that moves
// it from pending to a final status.
//
// The saga is an orchestration: this package decides every step and runs
// each one as a task on the PostgreSQL task queue, so a crashed process
// loses nothing and a step is retried with the same idempotency key until
// the remote side answers. docs/SAGA.md describes the steps, the
// compensation and every crash point.
package app

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Task kinds, one per saga step.
const (
	KindHold    = "hold"
	KindScreen  = "screen"
	KindCapture = "capture"
	KindRelease = "release"
)

// TaskKinds lists every task kind the saga uses.
var TaskKinds = []string{KindHold, KindScreen, KindCapture, KindRelease}

// sagaDedupKey makes each step a single task per transfer.
const sagaDedupKey = "saga"

// Config holds the tunables of the use cases.
type Config struct {
	// HoldTTL is how long the ledger keeps a hold if the saga never comes
	// back: the bound on money blocked by a stuck transfer.
	HoldTTL time.Duration
	// ScreenMaxFailures is the number of failed screening calls (timeouts,
	// errors, lost answers) after which the saga stops asking and releases
	// the hold.
	ScreenMaxFailures int
	// ScreenReviewTimeout is how long a screening may stay under review
	// before the saga gives up and releases the hold.
	ScreenReviewTimeout time.Duration
	// ScreenPollMin and ScreenPollMax clamp the provider's Retry-After for
	// a review.
	ScreenPollMin, ScreenPollMax time.Duration
	// WaitTimeout is the long-poll soft timeout.
	WaitTimeout time.Duration
	// WaitReadTimeout bounds each database read of a long poll.
	WaitReadTimeout time.Duration
}

func (c Config) validate() error {
	switch {
	case c.HoldTTL <= 0, c.ScreenReviewTimeout <= 0, c.ScreenPollMin <= 0, c.WaitTimeout <= 0, c.WaitReadTimeout <= 0:
		return errors.New("every duration must be positive")
	case c.ScreenPollMax < c.ScreenPollMin:
		return errors.New("ScreenPollMax must not be below ScreenPollMin")
	case c.ScreenMaxFailures < 1:
		return errors.New("ScreenMaxFailures must be at least 1")
	case c.HoldTTL <= c.ScreenReviewTimeout:
		return errors.New("HoldTTL must exceed ScreenReviewTimeout, or approved transfers find their hold expired")
	}
	return nil
}

// Deps lists the collaborators of Service. Every field is required.
type Deps struct {
	Tx        TxRunner
	Transfers TransferStore
	Tasks     Enqueuer
	Events    EventRecorder
	Ledger    Ledger
	Screener  Screener
	Watcher   Watcher
	Clock     Clock
	Metrics   Metrics
	// NewID generates ids; production uses uuid.NewV7.
	NewID  func() (uuid.UUID, error)
	Logger *slog.Logger
}

// Service implements the use cases.
type Service struct {
	tx        TxRunner
	transfers TransferStore
	tasks     Enqueuer
	events    EventRecorder
	ledger    Ledger
	screener  Screener
	watcher   Watcher
	clock     Clock
	metrics   Metrics
	newID     func() (uuid.UUID, error)
	log       *slog.Logger
	cfg       Config
}

// NewService returns a Service. It panics on a missing dependency or an
// invalid configuration: a wiring bug must fail at start, not on the first
// request.
func NewService(d Deps, cfg Config) *Service {
	if err := cfg.validate(); err != nil {
		panic("transfers/app: NewService: " + err.Error())
	}
	if d.Tx == nil || d.Transfers == nil || d.Tasks == nil || d.Events == nil || d.Ledger == nil ||
		d.Screener == nil || d.Watcher == nil || d.Clock == nil || d.Metrics == nil || d.NewID == nil || d.Logger == nil {
		panic("transfers/app: NewService: missing dependency")
	}
	return &Service{
		tx:        d.Tx,
		transfers: d.Transfers,
		tasks:     d.Tasks,
		events:    d.Events,
		ledger:    d.Ledger,
		screener:  d.Screener,
		watcher:   d.Watcher,
		clock:     d.Clock,
		metrics:   d.Metrics,
		newID:     d.NewID,
		log:       d.Logger,
		cfg:       cfg,
	}
}

func (s *Service) id() (uuid.UUID, error) {
	id, err := s.newID()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate id: %w", err)
	}
	return id, nil
}
