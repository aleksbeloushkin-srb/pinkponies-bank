// Package app implements the ledger's use cases: open accounts, place,
// capture and release holds, read balances and the journal, and release
// expired holds.
//
// Every mutating use case is idempotent by a client-supplied key and runs
// in one database transaction that locks the rows it changes. A replay of a
// known key returns the stored result with Replayed set; the gRPC adapter
// turns that into AlreadyExists with the original result attached.
//
// Lock order, which keeps concurrent use cases free of deadlocks: a hold
// row before account rows, and several account rows in ascending id order.
// PlaceHold locks only the source account.
package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// TxRunner runs fn inside one database transaction carried in ctx.
type TxRunner interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// AccountStore stores accounts.
type AccountStore interface {
	// InsertAccount stores a unless an account with the same idempotency key
	// exists, and returns the stored one either way.
	InsertAccount(ctx context.Context, a ledger.Account) (stored ledger.Account, inserted bool, err error)
	// Account returns ledger.ErrAccountNotFound for an unknown id.
	Account(ctx context.Context, id uuid.UUID) (ledger.Account, error)
	// LockAccounts reads the accounts and locks their rows, in ascending id
	// order, until the transaction in ctx ends. A missing id is
	// ledger.ErrAccountNotFound.
	LockAccounts(ctx context.Context, ids ...uuid.UUID) (map[uuid.UUID]ledger.Account, error)
	// LockTreasury returns the treasury of the candidate's currency, creating
	// it from candidate if it does not exist, and locks its row.
	LockTreasury(ctx context.Context, candidate ledger.Account) (ledger.Account, error)
	// SaveBalances persists the posted and held balances of the accounts.
	SaveBalances(ctx context.Context, accounts ...ledger.Account) error
	// Accounts lists accounts with an id greater than after, in id order.
	Accounts(ctx context.Context, after uuid.UUID, limit int) ([]ledger.Account, error)
}

// HoldStore stores holds.
type HoldStore interface {
	// HoldByKey finds the hold placed with an idempotency key.
	HoldByKey(ctx context.Context, key string) (ledger.Hold, bool, error)
	// InsertHold stores h unless a hold with the same idempotency key exists.
	InsertHold(ctx context.Context, h ledger.Hold) (inserted bool, err error)
	// LockHold reads the hold and locks its row until the transaction ends.
	LockHold(ctx context.Context, id uuid.UUID) (ledger.Hold, error)
	// TryLockHold is LockHold that skips a row locked by another
	// transaction instead of waiting; ok is false then.
	TryLockHold(ctx context.Context, id uuid.UUID) (h ledger.Hold, ok bool, err error)
	// SaveHold persists the hold's status fields.
	SaveHold(ctx context.Context, h ledger.Hold) error
	// ExpiredHolds lists up to limit active holds past their expiry at now.
	ExpiredHolds(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)
}

// Journal stores balanced transactions and their entries.
type Journal interface {
	// TransactionByKey finds a transaction, with its entries, by
	// idempotency key.
	TransactionByKey(ctx context.Context, key string) (ledger.Transaction, bool, error)
	// InsertTransaction stores the transaction and its entries and returns
	// it with the entry ids assigned.
	InsertTransaction(ctx context.Context, t ledger.Transaction) (ledger.Transaction, error)
	// Entries lists an account's entries with an id greater than after.
	Entries(ctx context.Context, accountID uuid.UUID, after int64, limit int) ([]ledger.Entry, error)
}

// Release causes, the cause label of ledger_holds_released_total.
const (
	CauseRequested = "requested"
	CauseExpired   = "expired"
)

// Metrics receives business measurements, only after the transaction that
// made the change has committed.
type Metrics interface {
	HoldPlaced()
	HoldCaptured()
	HoldReleased(cause string)
}

// Config holds the tunables of the use cases.
type Config struct {
	// DefaultHoldTTL applies when PlaceHold asks for no TTL.
	DefaultHoldTTL time.Duration
	// MaxHoldTTL caps the TTL a client may ask for, so that a forgotten
	// hold blocks money for a bounded time.
	MaxHoldTTL time.Duration
}

// Deps lists the collaborators of Service. Every field is required.
type Deps struct {
	Tx       TxRunner
	Accounts AccountStore
	Holds    HoldStore
	Journal  Journal
	Clock    Clock
	Metrics  Metrics
	// NewID generates ids; production uses uuid.NewV7, which keeps
	// primary-key inserts close together in the index.
	NewID  func() (uuid.UUID, error)
	Logger *slog.Logger
}

// Service implements the use cases.
type Service struct {
	tx       TxRunner
	accounts AccountStore
	holds    HoldStore
	journal  Journal
	clock    Clock
	metrics  Metrics
	newID    func() (uuid.UUID, error)
	log      *slog.Logger
	cfg      Config
}

// NewService returns a Service. It panics on a missing dependency or an
// invalid configuration: that is a wiring bug that must fail at start.
func NewService(d Deps, cfg Config) *Service {
	if err := cfg.validate(); err != nil {
		panic("ledger/app: NewService: " + err.Error())
	}
	if d.Tx == nil || d.Accounts == nil || d.Holds == nil || d.Journal == nil || d.Clock == nil ||
		d.Metrics == nil || d.NewID == nil || d.Logger == nil {
		panic("ledger/app: NewService: missing dependency")
	}
	return &Service{
		tx:       d.Tx,
		accounts: d.Accounts,
		holds:    d.Holds,
		journal:  d.Journal,
		clock:    d.Clock,
		metrics:  d.Metrics,
		newID:    d.NewID,
		log:      d.Logger,
		cfg:      cfg,
	}
}

func (c Config) validate() error {
	if c.DefaultHoldTTL <= 0 || c.MaxHoldTTL < c.DefaultHoldTTL {
		return errors.New("hold TTLs must be positive and DefaultHoldTTL must not exceed MaxHoldTTL")
	}
	return nil
}
