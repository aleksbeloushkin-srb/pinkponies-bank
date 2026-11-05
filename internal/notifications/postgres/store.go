// Package postgres implements the notifications service's storage on
// PostgreSQL with pgx.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// maxErrorLen truncates stored error messages.
const maxErrorLen = 500

// Store implements app.Store.
type Store struct {
	tx *txctx.Manager
}

// NewStore returns a Store over tx.
func NewStore(tx *txctx.Manager) *Store {
	return &Store{tx: tx}
}

// MarkProcessed inserts the event id unless it is there. ON CONFLICT DO
// NOTHING keeps the transaction usable for a duplicate; a concurrent insert
// of the same id waits for the other transaction and then does nothing.
func (s *Store) MarkProcessed(ctx context.Context, e app.ProcessedEvent) (bool, error) {
	tag, err := s.tx.DB(ctx).Exec(ctx, `
		INSERT INTO processed_events (event_id, transfer_id, type, topic, partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.TransferID, e.Type, e.Topic, e.Partition, e.Offset)
	if err != nil {
		return false, fmt.Errorf("insert processed event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertNotification stores n and a pending delivery per channel.
func (s *Store) InsertNotification(ctx context.Context, n notifications.Notification, channels []string) error {
	db := s.tx.DB(ctx)
	if _, err := db.Exec(ctx, `
		INSERT INTO notifications (id, event_id, account_id, transfer_id, kind, message, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		n.ID, n.EventID, n.AccountID, n.TransferID, string(n.Kind), n.Message, n.CreatedAt); err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	for _, ch := range channels {
		if _, err := db.Exec(ctx, `
			INSERT INTO deliveries (notification_id, channel, state, updated_at) VALUES ($1, $2, 'pending', $3)`,
			n.ID, ch, n.CreatedAt); err != nil {
			return fmt.Errorf("insert delivery: %w", err)
		}
	}
	return nil
}

const notificationColumns = `id, event_id, account_id, transfer_id, kind, message, created_at`

func scanNotification(row pgx.Row) (notifications.Notification, error) {
	var (
		n    notifications.Notification
		kind string
	)
	err := row.Scan(&n.ID, &n.EventID, &n.AccountID, &n.TransferID, &kind, &n.Message, &n.CreatedAt)
	n.Kind = notifications.Kind(kind)
	return n, err
}

// Notification reads one notification with its deliveries.
func (s *Store) Notification(ctx context.Context, id uuid.UUID) (notifications.Notification, error) {
	n, err := scanNotification(s.tx.DB(ctx).QueryRow(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return notifications.Notification{}, notifications.ErrNotFound
	}
	if err != nil {
		return notifications.Notification{}, fmt.Errorf("read notification: %w", err)
	}
	ds, err := s.deliveries(ctx, []uuid.UUID{id})
	if err != nil {
		return notifications.Notification{}, err
	}
	n.Deliveries = ds[id]
	return n, nil
}

// RecordAttempt stores the attempt and updates the delivery, in one
// transaction.
func (s *Store) RecordAttempt(ctx context.Context, id uuid.UUID, channel string, a app.Attempt) error {
	outcome, state := "delivered", notifications.DeliveryDelivered
	var errText *string
	switch {
	case a.GiveUp:
		outcome, state = "gave_up", notifications.DeliveryFailed
	case a.Err != nil:
		outcome, state = "failed", notifications.DeliveryPending
	}
	if a.Err != nil {
		msg := a.Err.Error()
		if len(msg) > maxErrorLen {
			msg = msg[:maxErrorLen]
		}
		errText = &msg
	}
	return s.tx.Run(ctx, func(ctx context.Context) error {
		db := s.tx.DB(ctx)
		if _, err := db.Exec(ctx, `
			INSERT INTO delivery_attempts (notification_id, channel, attempt, outcome, error, duration_ms, at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id, channel, a.Number, outcome, errText, a.Duration.Milliseconds(), a.At); err != nil {
			return fmt.Errorf("insert delivery attempt: %w", err)
		}
		var deliveredAt *time.Time
		if state == notifications.DeliveryDelivered {
			deliveredAt = &a.At
		}
		tag, err := db.Exec(ctx, `
			UPDATE deliveries
			SET state = $3, attempts = $4, last_error = $5, delivered_at = $6, updated_at = $7
			WHERE notification_id = $1 AND channel = $2 AND state = 'pending'`,
			id, channel, string(state), a.Number, errText, deliveredAt, a.At)
		if err != nil {
			return fmt.Errorf("update delivery: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("update delivery %s/%s: not pending", id, channel)
		}
		return nil
	})
}

// ByAccount lists an account's notifications, newest first, with their
// deliveries.
func (s *Store) ByAccount(ctx context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error) {
	rows, err := s.tx.DB(ctx).Query(ctx, `
		SELECT `+notificationColumns+` FROM notifications
		WHERE account_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`, account, limit)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (notifications.Notification, error) { return scanNotification(row) })
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	ids := make([]uuid.UUID, len(out))
	for i, n := range out {
		ids[i] = n.ID
	}
	ds, err := s.deliveries(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Deliveries = ds[out[i].ID]
	}
	return out, nil
}

func (s *Store) deliveries(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]notifications.Delivery, error) {
	rows, err := s.tx.DB(ctx).Query(ctx, `
		SELECT notification_id, channel, state, attempts, coalesce(last_error, ''), delivered_at
		FROM deliveries WHERE notification_id = ANY($1) ORDER BY channel`, ids)
	if err != nil {
		return nil, fmt.Errorf("read deliveries: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID][]notifications.Delivery{}
	for rows.Next() {
		var (
			id          uuid.UUID
			d           notifications.Delivery
			state       string
			deliveredAt *time.Time
		)
		if err := rows.Scan(&id, &d.Channel, &state, &d.Attempts, &d.LastError, &deliveredAt); err != nil {
			return nil, fmt.Errorf("scan delivery: %w", err)
		}
		d.State = notifications.DeliveryState(state)
		if deliveredAt != nil {
			d.DeliveredAt = *deliveredAt
		}
		out[id] = append(out[id], d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deliveries: %w", err)
	}
	return out, nil
}
