// Package postgres implements the ledger's storage on PostgreSQL with pgx.
//
// Every query goes through txctx.Manager.DB(ctx), so a call made inside
// txctx.Manager.Run joins the caller's transaction. Methods that lock rows
// refuse to run without a transaction, because SELECT ... FOR UPDATE in
// autocommit mode silently locks nothing.
//
// Row locks are FOR NO KEY UPDATE, not FOR UPDATE: balances and statuses
// change, keys never do. The weaker lock does not conflict with the
// FOR KEY SHARE locks that foreign-key checks take, so inserting a hold or
// an entry that references an account another transaction has locked does
// not wait for that transaction, which would open a path to deadlocks.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// errNoTx means a locking method was called outside a transaction.
var errNoTx = errors.New("postgres: row locks need a transaction in ctx")

// Store implements the account, hold and journal stores of the ledger.
type Store struct {
	tx *txctx.Manager
}

// NewStore returns a Store over tx.
func NewStore(tx *txctx.Manager) *Store {
	return &Store{tx: tx}
}

const accountColumns = `id, name, currency, kind, posted_minor, held_minor,
	coalesce(idempotency_key, ''), coalesce(request_hash, ''), created_at, updated_at`

func scanAccount(row pgx.Row) (ledger.Account, error) {
	var (
		a              ledger.Account
		currency, kind string
	)
	err := row.Scan(&a.ID, &a.Name, &currency, &kind, &a.PostedMinor, &a.HeldMinor,
		&a.IdempotencyKey, &a.RequestHash, &a.CreatedAt, &a.UpdatedAt)
	a.Currency, a.Kind = ledger.Currency(currency), ledger.AccountKind(kind)
	return a, err
}

// InsertAccount stores a customer account unless its idempotency key is
// taken, and returns the stored row either way. ON CONFLICT DO NOTHING
// keeps the surrounding transaction usable on a duplicate key; a unique
// violation would abort it.
func (s *Store) InsertAccount(ctx context.Context, a ledger.Account) (ledger.Account, bool, error) {
	db := s.tx.DB(ctx)
	tag, err := db.Exec(ctx, `
		INSERT INTO accounts (id, name, currency, kind, posted_minor, held_minor,
			idempotency_key, request_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		a.ID, a.Name, string(a.Currency), string(a.Kind), a.PostedMinor, a.HeldMinor,
		a.IdempotencyKey, a.RequestHash, a.CreatedAt, a.UpdatedAt)
	if err != nil {
		return ledger.Account{}, false, fmt.Errorf("insert account: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return a, true, nil
	}
	stored, err := scanAccount(db.QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE idempotency_key = $1`, a.IdempotencyKey))
	if err != nil {
		return ledger.Account{}, false, fmt.Errorf("read account by key: %w", err)
	}
	return stored, false, nil
}

// Account reads one account.
func (s *Store) Account(ctx context.Context, id uuid.UUID) (ledger.Account, error) {
	a, err := scanAccount(s.tx.DB(ctx).QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Account{}, fmt.Errorf("%w: %s", ledger.ErrAccountNotFound, id)
	}
	if err != nil {
		return ledger.Account{}, fmt.Errorf("read account: %w", err)
	}
	return a, nil
}

// LockAccounts reads and locks the accounts in ascending id order. The
// LockRows step of the plan runs above the sort, so the locks are taken in
// that order too, and two transactions that lock the same pair of accounts
// cannot deadlock.
func (s *Store) LockAccounts(ctx context.Context, ids ...uuid.UUID) (map[uuid.UUID]ledger.Account, error) {
	if !txctx.InTx(ctx) {
		return nil, errNoTx
	}
	rows, err := s.tx.DB(ctx).Query(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE id = ANY($1) ORDER BY id FOR NO KEY UPDATE`, ids)
	if err != nil {
		return nil, fmt.Errorf("lock accounts: %w", err)
	}
	locked, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ledger.Account, error) { return scanAccount(row) })
	if err != nil {
		return nil, fmt.Errorf("lock accounts: %w", err)
	}
	out := make(map[uuid.UUID]ledger.Account, len(locked))
	for _, a := range locked {
		out[a.ID] = a
	}
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			return nil, fmt.Errorf("%w: %s", ledger.ErrAccountNotFound, id)
		}
	}
	return out, nil
}

// LockTreasury returns the treasury of the candidate's currency, inserting
// the candidate if there is none yet, and locks its row.
func (s *Store) LockTreasury(ctx context.Context, candidate ledger.Account) (ledger.Account, error) {
	if !txctx.InTx(ctx) {
		return ledger.Account{}, errNoTx
	}
	db := s.tx.DB(ctx)
	key := ledger.TreasuryKey(candidate.Currency)
	if _, err := db.Exec(ctx, `
		INSERT INTO accounts (id, name, currency, kind, system_key, created_at, updated_at)
		VALUES ($1, $2, $3, 'system', $4, $5, $6)
		ON CONFLICT (system_key) DO NOTHING`,
		candidate.ID, candidate.Name, string(candidate.Currency), key, candidate.CreatedAt, candidate.UpdatedAt); err != nil {
		return ledger.Account{}, fmt.Errorf("insert treasury: %w", err)
	}
	a, err := scanAccount(db.QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE system_key = $1 FOR NO KEY UPDATE`, key))
	if err != nil {
		return ledger.Account{}, fmt.Errorf("lock treasury: %w", err)
	}
	return a, nil
}

// SaveBalances writes the posted and held balances of accounts.
func (s *Store) SaveBalances(ctx context.Context, accounts ...ledger.Account) error {
	db := s.tx.DB(ctx)
	for _, a := range accounts {
		tag, err := db.Exec(ctx,
			`UPDATE accounts SET posted_minor = $2, held_minor = $3, updated_at = $4 WHERE id = $1`,
			a.ID, a.PostedMinor, a.HeldMinor, a.UpdatedAt)
		if err != nil {
			return fmt.Errorf("save account %s: %w", a.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("save account %s: %w", a.ID, ledger.ErrAccountNotFound)
		}
	}
	return nil
}

// Accounts lists accounts after the given id, in id order.
func (s *Store) Accounts(ctx context.Context, after uuid.UUID, limit int) ([]ledger.Account, error) {
	rows, err := s.tx.DB(ctx).Query(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE id > $1 ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ledger.Account, error) { return scanAccount(row) })
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	return out, nil
}

const holdColumns = `id, account_id, destination_account_id, amount_minor, currency, status,
	idempotency_key, request_hash, coalesce(release_reason, ''), coalesce(release_key, ''),
	transaction_id, expires_at, created_at, updated_at`

func scanHold(row pgx.Row) (ledger.Hold, error) {
	var (
		h                ledger.Hold
		currency, status string
		txID             pgtype.UUID
	)
	err := row.Scan(&h.ID, &h.AccountID, &h.DestinationID, &h.AmountMinor, &currency, &status,
		&h.IdempotencyKey, &h.RequestHash, &h.ReleaseReason, &h.ReleaseKey,
		&txID, &h.ExpiresAt, &h.CreatedAt, &h.UpdatedAt)
	h.Currency, h.Status = ledger.Currency(currency), ledger.HoldStatus(status)
	if txID.Valid {
		h.TransactionID = txID.Bytes
	}
	return h, err
}

// HoldByKey finds the hold placed with key.
func (s *Store) HoldByKey(ctx context.Context, key string) (ledger.Hold, bool, error) {
	h, err := scanHold(s.tx.DB(ctx).QueryRow(ctx,
		`SELECT `+holdColumns+` FROM holds WHERE idempotency_key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, false, nil
	}
	if err != nil {
		return ledger.Hold{}, false, fmt.Errorf("read hold by key: %w", err)
	}
	return h, true, nil
}

// InsertHold stores h unless its idempotency key is taken.
func (s *Store) InsertHold(ctx context.Context, h ledger.Hold) (bool, error) {
	tag, err := s.tx.DB(ctx).Exec(ctx, `
		INSERT INTO holds (id, account_id, destination_account_id, amount_minor, currency, status,
			idempotency_key, request_hash, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		h.ID, h.AccountID, h.DestinationID, h.AmountMinor, string(h.Currency), string(h.Status),
		h.IdempotencyKey, h.RequestHash, h.ExpiresAt, h.CreatedAt, h.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("insert hold: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// LockHold reads the hold and locks its row.
func (s *Store) LockHold(ctx context.Context, id uuid.UUID) (ledger.Hold, error) {
	if !txctx.InTx(ctx) {
		return ledger.Hold{}, errNoTx
	}
	h, err := scanHold(s.tx.DB(ctx).QueryRow(ctx,
		`SELECT `+holdColumns+` FROM holds WHERE id = $1 FOR NO KEY UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, fmt.Errorf("%w: %s", ledger.ErrHoldNotFound, id)
	}
	if err != nil {
		return ledger.Hold{}, fmt.Errorf("lock hold: %w", err)
	}
	return h, nil
}

// TryLockHold locks the hold unless another transaction holds its row.
func (s *Store) TryLockHold(ctx context.Context, id uuid.UUID) (ledger.Hold, bool, error) {
	if !txctx.InTx(ctx) {
		return ledger.Hold{}, false, errNoTx
	}
	h, err := scanHold(s.tx.DB(ctx).QueryRow(ctx,
		`SELECT `+holdColumns+` FROM holds WHERE id = $1 FOR NO KEY UPDATE SKIP LOCKED`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Hold{}, false, nil
	}
	if err != nil {
		return ledger.Hold{}, false, fmt.Errorf("try lock hold: %w", err)
	}
	return h, true, nil
}

// SaveHold writes the hold's status fields.
func (s *Store) SaveHold(ctx context.Context, h ledger.Hold) error {
	tag, err := s.tx.DB(ctx).Exec(ctx, `
		UPDATE holds
		SET status = $2, release_reason = nullif($3, ''), release_key = nullif($4, ''),
			transaction_id = $5, updated_at = $6
		WHERE id = $1`,
		h.ID, string(h.Status), h.ReleaseReason, h.ReleaseKey, nullUUID(h.TransactionID), h.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save hold %s: %w", h.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("save hold %s: %w", h.ID, ledger.ErrHoldNotFound)
	}
	return nil
}

// ExpiredHolds lists active holds whose expiry is not after now.
func (s *Store) ExpiredHolds(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := s.tx.DB(ctx).Query(ctx, `
		SELECT id FROM holds
		WHERE status = 'active' AND expires_at <= $1
		ORDER BY expires_at
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired holds: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("list expired holds: %w", err)
	}
	return ids, nil
}

// TransactionByKey reads a transaction and its entries by idempotency key.
func (s *Store) TransactionByKey(ctx context.Context, key string) (ledger.Transaction, bool, error) {
	db := s.tx.DB(ctx)
	var (
		t        ledger.Transaction
		kind     string
		currency string
		holdID   pgtype.UUID
	)
	err := db.QueryRow(ctx, `
		SELECT id, kind, idempotency_key, hold_id, currency, created_at
		FROM transactions WHERE idempotency_key = $1`, key).
		Scan(&t.ID, &kind, &t.IdempotencyKey, &holdID, &currency, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transaction{}, false, nil
	}
	if err != nil {
		return ledger.Transaction{}, false, fmt.Errorf("read transaction by key: %w", err)
	}
	t.Kind, t.Currency = ledger.TransactionKind(kind), ledger.Currency(currency)
	if holdID.Valid {
		t.HoldID = holdID.Bytes
	}
	rows, err := db.Query(ctx, `SELECT `+entryColumns+` FROM entries WHERE transaction_id = $1 ORDER BY id`, t.ID)
	if err != nil {
		return ledger.Transaction{}, false, fmt.Errorf("read entries: %w", err)
	}
	if t.Entries, err = pgx.CollectRows(rows, scanEntry); err != nil {
		return ledger.Transaction{}, false, fmt.Errorf("read entries: %w", err)
	}
	return t, true, nil
}

// InsertTransaction stores t and its entries. The deferred trigger
// entries_balanced checks at commit that the entries sum to zero.
func (s *Store) InsertTransaction(ctx context.Context, t ledger.Transaction) (ledger.Transaction, error) {
	db := s.tx.DB(ctx)
	if _, err := db.Exec(ctx, `
		INSERT INTO transactions (id, kind, idempotency_key, hold_id, currency, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		t.ID, string(t.Kind), t.IdempotencyKey, nullUUID(t.HoldID), string(t.Currency), t.CreatedAt); err != nil {
		return ledger.Transaction{}, fmt.Errorf("insert transaction: %w", err)
	}
	entries := make([]ledger.Entry, len(t.Entries))
	for i, e := range t.Entries {
		if err := db.QueryRow(ctx, `
			INSERT INTO entries (transaction_id, account_id, amount_minor, balance_after_minor, created_at)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id`,
			t.ID, e.AccountID, e.AmountMinor, e.BalanceAfterMinor, e.CreatedAt).Scan(&e.ID); err != nil {
			return ledger.Transaction{}, fmt.Errorf("insert entry: %w", err)
		}
		entries[i] = e
	}
	t.Entries = entries
	return t, nil
}

const entryColumns = `id, transaction_id, account_id, amount_minor, balance_after_minor, created_at`

func scanEntry(row pgx.CollectableRow) (ledger.Entry, error) {
	var e ledger.Entry
	err := row.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.AmountMinor, &e.BalanceAfterMinor, &e.CreatedAt)
	return e, err
}

// Entries lists an account's entries after the given entry id.
func (s *Store) Entries(ctx context.Context, accountID uuid.UUID, after int64, limit int) ([]ledger.Entry, error) {
	rows, err := s.tx.DB(ctx).Query(ctx,
		`SELECT `+entryColumns+` FROM entries WHERE account_id = $1 AND id > $2 ORDER BY id LIMIT $3`,
		accountID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list entries: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanEntry)
	if err != nil {
		return nil, fmt.Errorf("list entries: %w", err)
	}
	return out, nil
}

func nullUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: id != uuid.Nil}
}
