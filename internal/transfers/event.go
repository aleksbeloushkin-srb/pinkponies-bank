package transfers

import (
	"time"

	"github.com/google/uuid"
)

// EventType names an integration event; one per status.
type EventType string

// Event types. They are published on the transfers.events topic.
const (
	EventCreated   EventType = "transfer.created"
	EventHeld      EventType = "transfer.held"
	EventCompleted EventType = "transfer.completed"
	EventRejected  EventType = "transfer.rejected"
	EventCanceled  EventType = "transfer.canceled"
)

var eventByStatus = map[Status]EventType{
	StatusPending:   EventCreated,
	StatusHeld:      EventHeld,
	StatusCompleted: EventCompleted,
	StatusRejected:  EventRejected,
	StatusCanceled:  EventCanceled,
}

// Event is a status change of a transfer, written to the outbox in the
// same transaction as the change.
type Event struct {
	ID         uuid.UUID
	Type       EventType
	Transfer   Transfer
	OccurredAt time.Time
}

// NewEvent snapshots t into the event of its current status.
func NewEvent(id uuid.UUID, t Transfer) Event {
	return Event{ID: id, Type: eventByStatus[t.Status], Transfer: t, OccurredAt: t.UpdatedAt}
}
