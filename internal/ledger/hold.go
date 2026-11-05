package ledger

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// HoldStatus is the lifecycle state of a hold.
//
//	active ─┬─► captured   (money moved to the destination)
//	        └─► released   (on request, or because it expired)
//
// captured and released are final.
type HoldStatus string

// Hold statuses.
const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldReleased HoldStatus = "released"
)

// ReasonExpired is the release reason recorded by the expiry sweep.
const ReasonExpired = "expired"

// Hold reserves an amount on a source account for a transfer to a
// destination account. Placing it lowers the source's available balance;
// capturing it posts the transfer; releasing it gives the amount back.
//
// The destination is fixed when the hold is placed, so a capture can only
// fail for technical reasons or because the hold is gone, never because of
// a bad destination: the point of no return of a transfer must not depend
// on input checked that late.
type Hold struct {
	ID            uuid.UUID
	AccountID     uuid.UUID
	DestinationID uuid.UUID
	AmountMinor   int64
	Currency      Currency
	Status        HoldStatus
	// IdempotencyKey and RequestHash identify the PlaceHold request.
	IdempotencyKey string
	RequestHash    string
	// ReleaseReason and ReleaseKey are set when the hold is released.
	ReleaseReason string
	ReleaseKey    string
	// TransactionID is the journal transaction that captured the hold.
	TransactionID uuid.UUID
	ExpiresAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewHold builds an active hold. The caller reserves the amount on the
// source account in the same transaction.
func NewHold(id, accountID, destinationID uuid.UUID, amountMinor int64, currency Currency,
	idempotencyKey, requestHash string, now, expiresAt time.Time) (Hold, error) {
	if err := validateAmount("amount_minor", amountMinor); err != nil {
		return Hold{}, err
	}
	if accountID == destinationID {
		return Hold{}, &ValidationError{Field: "destination_account_id", Reason: "must differ from the source account"}
	}
	if !expiresAt.After(now) {
		return Hold{}, &ValidationError{Field: "ttl", Reason: "must be positive"}
	}
	return Hold{
		ID:             id,
		AccountID:      accountID,
		DestinationID:  destinationID,
		AmountMinor:    amountMinor,
		Currency:       currency,
		Status:         HoldActive,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
		ExpiresAt:      expiresAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// Expired reports whether the hold is past its expiry time at now.
func (h Hold) Expired(now time.Time) bool {
	return !now.Before(h.ExpiresAt)
}

// CheckCapturable returns nil if the hold can be captured at now.
func (h Hold) CheckCapturable(now time.Time) error {
	switch h.Status {
	case HoldCaptured:
		return ErrHoldCaptured
	case HoldReleased:
		return ErrHoldReleased
	}
	if h.Expired(now) {
		return ErrHoldExpired
	}
	return nil
}

// MarkCaptured records the capturing transaction. Call CheckCapturable
// first.
func (h *Hold) MarkCaptured(transactionID uuid.UUID, now time.Time) {
	h.Status = HoldCaptured
	h.TransactionID = transactionID
	h.UpdatedAt = now
}

// Release moves an active hold to released. Releasing a released hold is a
// no-op (changed is false), which makes compensation idempotent whoever
// released it first; releasing a captured hold is an error, because the
// money has already moved.
func (h *Hold) Release(reason, key string, now time.Time) (changed bool, err error) {
	switch h.Status {
	case HoldReleased:
		return false, nil
	case HoldCaptured:
		return false, ErrHoldCaptured
	}
	h.Status = HoldReleased
	h.ReleaseReason = reason
	h.ReleaseKey = key
	h.UpdatedAt = now
	return true, nil
}

// LogValue keeps the idempotency key and the hash out of logs.
func (h Hold) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", h.ID.String()),
		slog.String("account_id", h.AccountID.String()),
		slog.String("status", string(h.Status)),
		slog.Int64("amount_minor", h.AmountMinor),
		slog.String("currency", string(h.Currency)),
	)
}
