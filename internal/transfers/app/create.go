package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// MaxIdempotencyKeyLen bounds the Idempotency-Key header.
const MaxIdempotencyKeyLen = 255

// CreateTransferRequest is the input of CreateTransfer.
type CreateTransferRequest struct {
	IdempotencyKey       string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             string
}

// CreateTransferResult is the stored transfer and whether this call
// created it. Created is false for a replay of an earlier request.
type CreateTransferResult struct {
	Transfer transfers.Transfer
	Created  bool
}

// CreateTransfer accepts a transfer idempotently. One transaction inserts
// the pending transfer, schedules the hold step and records
// transfer.created, so either all three exist or none does.
//
// A known Idempotency-Key returns the stored transfer when the body is the
// same, and transfers.ErrIdempotencyConflict when it differs. Accounts are
// not checked here: the ledger checks them when it places the hold, and an
// unknown account rejects the transfer then.
func (s *Service) CreateTransfer(ctx context.Context, req CreateTransferRequest) (CreateTransferResult, error) {
	if err := validateIdempotencyKey(req.IdempotencyKey); err != nil {
		return CreateTransferResult{}, err
	}
	id, err := s.id()
	if err != nil {
		return CreateTransferResult{}, err
	}
	eventID, err := s.id()
	if err != nil {
		return CreateTransferResult{}, err
	}
	now := s.clock.Now()
	t, err := transfers.New(id, req.SourceAccountID, req.DestinationAccountID, req.AmountMinor, req.Currency,
		req.IdempotencyKey, requestHash(req), now)
	if err != nil {
		return CreateTransferResult{}, err
	}

	var res CreateTransferResult
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		stored, inserted, err := s.transfers.Insert(ctx, t)
		if err != nil {
			return fmt.Errorf("store transfer: %w", err)
		}
		if !inserted {
			if stored.RequestHash != t.RequestHash {
				return transfers.ErrIdempotencyConflict
			}
			res = CreateTransferResult{Transfer: stored}
			return nil
		}
		if _, err := s.tasks.Enqueue(ctx, s.step(KindHold, id, nil)); err != nil {
			return fmt.Errorf("enqueue hold: %w", err)
		}
		if err := s.events.Record(ctx, transfers.NewEvent(eventID, stored)); err != nil {
			return fmt.Errorf("record event: %w", err)
		}
		res = CreateTransferResult{Transfer: stored, Created: true}
		return nil
	})
	if err != nil {
		return CreateTransferResult{}, err
	}
	if res.Created {
		s.metrics.TransferCreated()
		s.log.InfoContext(ctx, "transfer created", "transfer", res.Transfer)
	}
	return res, nil
}

// step builds the task of a saga step, due now.
func (s *Service) step(kind string, id uuid.UUID, payload json.RawMessage) taskqueue.Spec {
	return taskqueue.Spec{Kind: kind, AggregateID: id, DedupKey: sagaDedupKey, Payload: payload, RunAfter: s.clock.Now()}
}

func validateIdempotencyKey(key string) error {
	if key == "" || len(key) > MaxIdempotencyKeyLen {
		return &transfers.ValidationError{
			Field:  "Idempotency-Key",
			Reason: fmt.Sprintf("header is required and must be at most %d characters", MaxIdempotencyKeyLen),
		}
	}
	for _, r := range key {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return &transfers.ValidationError{Field: "Idempotency-Key", Reason: "must be printable ASCII"}
		}
	}
	return nil
}

// requestHash fingerprints the business content of a create request. JSON
// encoding of a fixed struct is an unambiguous canonical form.
func requestHash(req CreateTransferRequest) string {
	canonical, err := json.Marshal(struct {
		Source      string `json:"source_account_id"`
		Destination string `json:"destination_account_id"`
		Amount      int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
	}{req.SourceAccountID.String(), req.DestinationAccountID.String(), req.AmountMinor, req.Currency})
	if err != nil {
		panic("transfers/app: requestHash: " + err.Error())
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
