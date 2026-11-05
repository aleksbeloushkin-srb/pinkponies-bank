package postgres

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// OutboxRecord is a stored event waiting to be published.
type OutboxRecord struct {
	ID          int64
	EventID     uuid.UUID
	AggregateID uuid.UUID
	Type        string
	Payload     []byte
	CreatedAt   time.Time
}

// Outbox stores integration events next to the data they describe.
type Outbox struct {
	tx    *txctx.Manager
	ready chan struct{}
}

// NewOutbox returns an outbox store.
func NewOutbox(tx *txctx.Manager) *Outbox {
	return &Outbox{tx: tx, ready: make(chan struct{}, 1)}
}

// Ready receives a value after a transaction that recorded an event
// commits, so the local relay can publish without waiting for its next
// poll.
func (o *Outbox) Ready() <-chan struct{} {
	return o.ready
}

func (o *Outbox) signal(context.Context) {
	select {
	case o.ready <- struct{}{}:
	default:
	}
}

// Record encodes e in the eventsv1 wire format and inserts it in the
// caller's transaction. The mapping from the domain to the contract
// happens here, at the boundary, and nowhere else.
func (o *Outbox) Record(ctx context.Context, e transfers.Event) error {
	payload, err := eventsv1.Encode(toContract(e))
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	_, err = o.tx.DB(ctx).Exec(ctx, `
		INSERT INTO outbox (event_id, aggregate_id, type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.Transfer.ID, string(e.Type), payload, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	txctx.AfterCommit(ctx, o.signal)
	return nil
}

func toContract(e transfers.Event) eventsv1.TransferEvent {
	t := e.Transfer
	return eventsv1.TransferEvent{
		ID:            e.ID,
		Type:          eventsv1.Type(e.Type),
		SchemaVersion: eventsv1.SchemaVersion,
		OccurredAt:    e.OccurredAt.UTC(),
		Transfer: eventsv1.Transfer{
			ID:                   t.ID,
			SourceAccountID:      t.SourceAccountID,
			DestinationAccountID: t.DestinationAccountID,
			AmountMinor:          t.AmountMinor,
			Currency:             t.Currency,
			Status:               string(t.Status),
			Reason:               string(t.Reason),
			Version:              t.Version,
		},
	}
}

// Claim leases up to limit unpublished events in id order. It is one
// statement that commits at once: the relay holds no transaction and no row
// lock while it publishes. Concurrent relays skip each other's rows, and an
// event whose lease expired (a relay crashed mid-batch) can be claimed
// again, which makes delivery at least once.
func (o *Outbox) Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]OutboxRecord, error) {
	rows, err := o.tx.DB(ctx).Query(ctx, `
		WITH batch AS MATERIALIZED (
			SELECT id FROM outbox
			WHERE published_at IS NULL
			  AND (claimed_until IS NULL OR claimed_until <= $1)
			ORDER BY id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET claimed_until = $2
		FROM batch
		WHERE o.id = batch.id
		RETURNING o.id, o.event_id, o.aggregate_id, o.type, o.payload, o.created_at`,
		now, now.Add(lease), limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()

	var out []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.EventID, &r.AggregateID, &r.Type, &r.Payload, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	// RETURNING has no defined order; publishing must follow the id order.
	slices.SortFunc(out, func(a, b OutboxRecord) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

// MarkPublished stamps the given events as published.
func (o *Outbox) MarkPublished(ctx context.Context, ids []int64, at time.Time) error {
	_, err := o.tx.DB(ctx).Exec(ctx,
		`UPDATE outbox SET published_at = $2, claimed_until = NULL WHERE id = ANY($1)`, ids, at)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// Release returns claimed but unpublished events to the queue at once,
// instead of letting them wait for the lease to expire.
func (o *Outbox) Release(ctx context.Context, ids []int64) error {
	_, err := o.tx.DB(ctx).Exec(ctx,
		`UPDATE outbox SET claimed_until = NULL WHERE id = ANY($1) AND published_at IS NULL`, ids)
	if err != nil {
		return fmt.Errorf("release outbox: %w", err)
	}
	return nil
}
