// Package postgres implements the transfers service's storage on
// PostgreSQL with pgx: transfers with their saga log, the version reader of
// long polling, the outbox and the database gauges.
//
// Every query goes through txctx.Manager.DB(ctx), so a call made inside
// txctx.Manager.Run joins the caller's transaction. Lock refuses to run
// without one.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

var errNoTx = errors.New("postgres: row locks need a transaction in ctx")

// Transfers stores transfers and their saga log.
type Transfers struct {
	tx *txctx.Manager
}

// NewTransfers returns the transfer store.
func NewTransfers(tx *txctx.Manager) *Transfers {
	return &Transfers{tx: tx}
}

const transferColumns = `id, source_account_id, destination_account_id, amount_minor, currency, status,
	coalesce(reason, ''), version, hold_id, ledger_transaction_id, coalesce(screening_id, ''),
	idempotency_key, request_hash, created_at, updated_at`

func scanTransfer(row pgx.Row) (transfers.Transfer, error) {
	var (
		t              transfers.Transfer
		status, reason string
		hold, ledgerTx pgtype.UUID
	)
	err := row.Scan(&t.ID, &t.SourceAccountID, &t.DestinationAccountID, &t.AmountMinor, &t.Currency, &status,
		&reason, &t.Version, &hold, &ledgerTx, &t.ScreeningID, &t.IdempotencyKey, &t.RequestHash, &t.CreatedAt, &t.UpdatedAt)
	t.Status, t.Reason = transfers.Status(status), transfers.Reason(reason)
	if hold.Valid {
		t.HoldID = hold.Bytes
	}
	if ledgerTx.Valid {
		t.LedgerTransactionID = ledgerTx.Bytes
	}
	return t, err
}

// Insert stores t unless its idempotency key is taken, and returns the
// stored transfer either way. ON CONFLICT DO NOTHING keeps the surrounding
// transaction usable on a duplicate key.
func (s *Transfers) Insert(ctx context.Context, t transfers.Transfer) (transfers.Transfer, bool, error) {
	db := s.tx.DB(ctx)
	tag, err := db.Exec(ctx, `
		INSERT INTO transfers (id, source_account_id, destination_account_id, amount_minor, currency, status,
			version, idempotency_key, request_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		t.ID, t.SourceAccountID, t.DestinationAccountID, t.AmountMinor, t.Currency, string(t.Status),
		t.Version, t.IdempotencyKey, t.RequestHash, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return transfers.Transfer{}, false, fmt.Errorf("insert transfer: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return t, true, nil
	}
	stored, err := s.read(ctx, `SELECT `+transferColumns+` FROM transfers WHERE idempotency_key = $1`, t.IdempotencyKey)
	if err != nil {
		return transfers.Transfer{}, false, fmt.Errorf("read transfer by key: %w", err)
	}
	return stored, false, nil
}

// Transfer reads a transfer with its saga log.
func (s *Transfers) Transfer(ctx context.Context, id uuid.UUID) (transfers.Transfer, error) {
	return s.read(ctx, `SELECT `+transferColumns+` FROM transfers WHERE id = $1`, id)
}

// Lock reads the transfer and locks its row until the transaction ends.
// FOR NO KEY UPDATE is enough: the key never changes, and the weaker lock
// does not block foreign-key checks of saga_steps inserts.
func (s *Transfers) Lock(ctx context.Context, id uuid.UUID) (transfers.Transfer, error) {
	if !txctx.InTx(ctx) {
		return transfers.Transfer{}, errNoTx
	}
	return s.read(ctx, `SELECT `+transferColumns+` FROM transfers WHERE id = $1 FOR NO KEY UPDATE`, id)
}

func (s *Transfers) read(ctx context.Context, sql string, arg any) (transfers.Transfer, error) {
	db := s.tx.DB(ctx)
	t, err := scanTransfer(db.QueryRow(ctx, sql, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return transfers.Transfer{}, transfers.ErrNotFound
	}
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("read transfer: %w", err)
	}
	rows, err := db.Query(ctx, `
		SELECT step, state, coalesce(outcome, ''), attempts, started_at, updated_at
		FROM saga_steps WHERE transfer_id = $1 ORDER BY position`, t.ID)
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("read saga steps: %w", err)
	}
	t.Steps, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (transfers.StepRecord, error) {
		var (
			r           transfers.StepRecord
			step, state string
		)
		err := row.Scan(&step, &state, &r.Outcome, &r.Attempts, &r.StartedAt, &r.UpdatedAt)
		r.Step, r.State = transfers.Step(step), transfers.StepState(state)
		return r, err
	})
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("read saga steps: %w", err)
	}
	return t, nil
}

// Update writes t and its saga log if the stored version is still
// expectedVersion. The check is on top of the row lock the use cases hold:
// a code path that forgets the lock fails loudly instead of losing an
// update.
func (s *Transfers) Update(ctx context.Context, t transfers.Transfer, expectedVersion int64) error {
	db := s.tx.DB(ctx)
	tag, err := db.Exec(ctx, `
		UPDATE transfers
		SET status = $3, reason = nullif($4, ''), version = $5, hold_id = $6, ledger_transaction_id = $7,
			screening_id = nullif($8, ''), updated_at = $9
		WHERE id = $1 AND version = $2`,
		t.ID, expectedVersion, string(t.Status), string(t.Reason), t.Version,
		nullUUID(t.HoldID), nullUUID(t.LedgerTransactionID), t.ScreeningID, t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update transfer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return transfers.ErrVersionConflict
	}
	for i, r := range t.Steps {
		if _, err := db.Exec(ctx, `
			INSERT INTO saga_steps (transfer_id, step, state, outcome, attempts, idempotency_key, position, started_at, updated_at)
			VALUES ($1, $2, $3, nullif($4, ''), $5, $6, $7, $8, $9)
			ON CONFLICT (transfer_id, step) DO UPDATE
			SET state = excluded.state, outcome = excluded.outcome, attempts = excluded.attempts, updated_at = excluded.updated_at`,
			t.ID, string(r.Step), string(r.State), r.Outcome, r.Attempts, transfers.StepKey(t.ID, r.Step), i,
			r.StartedAt, r.UpdatedAt); err != nil {
			return fmt.Errorf("save saga step %s: %w", r.Step, err)
		}
	}
	return nil
}

// Versions returns the current versions of the given transfers, for the
// long-poll batcher: one query for every waiting client of the instance.
// Unknown ids are absent from the map.
func (s *Transfers) Versions(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error) {
	rows, err := s.tx.DB(ctx).Query(ctx, `SELECT id, version FROM transfers WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	defer rows.Close()
	out := make(map[uuid.UUID]int64, len(ids))
	for rows.Next() {
		var (
			id uuid.UUID
			v  int64
		)
		if err := rows.Scan(&id, &v); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		out[id] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read versions: %w", err)
	}
	return out, nil
}

func nullUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: id != uuid.Nil}
}
