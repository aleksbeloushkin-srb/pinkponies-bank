// Package eventsv1 is the contract of the transfers.events topic: the JSON
// schema of the events the transfers service publishes, the Kafka headers
// that go with them and the topic names. Producer and consumers depend on
// this package and on nothing of each other.
//
// Delivery is at least once. A consumer deduplicates by ID and must
// tolerate gaps in Transfer.Version: it grows with every change of a
// transfer, while events are published for status changes only.
//
// Compatibility rules for version 1: fields may be added, never removed or
// renamed, and consumers ignore fields they do not know. A breaking change
// is a new SchemaVersion and a new package.
package eventsv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
)

// Topic names.
const (
	// TopicTransfers carries one event per transfer status change, keyed by
	// transfer id so that the events of one transfer stay in order.
	TopicTransfers = "transfers.events"
	// TopicTransfersDLQ receives events that a consumer could not process
	// after its retries, with headers that say why.
	TopicTransfersDLQ = "transfers.events.dlq"
)

// Kafka header names.
const (
	HeaderEventID     = "event-id"
	HeaderEventType   = "event-type"
	HeaderContentType = "content-type"
	ContentType       = "application/json"
)

// SchemaVersion is the version of the event body described here.
const SchemaVersion = 1

// Type is an event type.
type Type string

// Event types, one per transfer status.
const (
	TransferCreated   Type = "transfer.created"
	TransferHeld      Type = "transfer.held"
	TransferCompleted Type = "transfer.completed"
	TransferRejected  Type = "transfer.rejected"
	TransferCanceled  Type = "transfer.canceled"
)

// Types lists every event type.
var Types = []Type{TransferCreated, TransferHeld, TransferCompleted, TransferRejected, TransferCanceled}

// TransferEvent is the body of a message on TopicTransfers.
type TransferEvent struct {
	// ID identifies the event; consumers deduplicate by it.
	ID            uuid.UUID `json:"id"`
	Type          Type      `json:"type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	Transfer      Transfer  `json:"transfer"`
}

// Transfer is the transfer as of the event.
type Transfer struct {
	ID                   uuid.UUID `json:"id"`
	SourceAccountID      uuid.UUID `json:"source_account_id"`
	DestinationAccountID uuid.UUID `json:"destination_account_id"`
	AmountMinor          int64     `json:"amount_minor"`
	Currency             string    `json:"currency"`
	Status               string    `json:"status"`
	// Reason says why a transfer was rejected or canceled.
	Reason  string `json:"reason,omitempty"`
	Version int64  `json:"version"`
}

// ErrInvalid wraps every validation failure of Decode and Validate.
var ErrInvalid = errors.New("invalid transfer event")

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// Validate checks the fields a consumer relies on.
func (e TransferEvent) Validate() error {
	fail := func(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }
	switch {
	case e.ID == uuid.Nil:
		return fail("id is missing")
	case !slices.Contains(Types, e.Type):
		return fail(fmt.Sprintf("unknown type %q", e.Type))
	case e.SchemaVersion != SchemaVersion:
		return fail(fmt.Sprintf("schema version %d is not %d", e.SchemaVersion, SchemaVersion))
	case e.OccurredAt.IsZero():
		return fail("occurred_at is missing")
	case e.Transfer.ID == uuid.Nil || e.Transfer.SourceAccountID == uuid.Nil || e.Transfer.DestinationAccountID == uuid.Nil:
		return fail("transfer ids are missing")
	case e.Transfer.AmountMinor <= 0:
		return fail("amount must be positive")
	case !currencyPattern.MatchString(e.Transfer.Currency):
		return fail("currency must be an ISO 4217 code")
	case e.Transfer.Status == "" || e.Transfer.Version <= 0:
		return fail("status and version are required")
	}
	return nil
}

// Encode returns the JSON body of e after validating it.
func Encode(e TransferEvent) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// Decode parses and validates a message body. Unknown fields are ignored,
// which is what lets the producer add fields without breaking consumers.
func Decode(b []byte) (TransferEvent, error) {
	var e TransferEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return TransferEvent{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := e.Validate(); err != nil {
		return TransferEvent{}, err
	}
	return e, nil
}
