// Package notifications is the domain model of the notifications service:
// what to tell which account when a transfer changes, and the delivery of
// each notification over a channel.
//
// The service consumes the transfers.events topic (api/events/v1) and
// imports nothing else of the transfers service.
package notifications

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
)

// ErrInvalidEvent marks a message that cannot be processed, whatever the
// number of tries: a poison message. The consumer sends it to the
// dead-letter topic after its retries.
var ErrInvalidEvent = errors.New("invalid event")

// ErrNotFound means the notification does not exist.
var ErrNotFound = errors.New("notification not found")

// Kind says what a notification is about.
type Kind string

// Notification kinds.
const (
	KindAccepted Kind = "transfer_accepted"
	KindSent     Kind = "transfer_sent"
	KindReceived Kind = "transfer_received"
	KindRejected Kind = "transfer_rejected"
	KindCanceled Kind = "transfer_canceled"
)

// Kinds lists every kind.
var Kinds = []Kind{KindAccepted, KindSent, KindReceived, KindRejected, KindCanceled}

// DeliveryState is the state of one notification on one channel.
type DeliveryState string

// Delivery states.
const (
	DeliveryPending   DeliveryState = "pending"
	DeliveryDelivered DeliveryState = "delivered"
	// DeliveryFailed means the channel kept failing and the service gave
	// up; the notification stays readable in the API.
	DeliveryFailed DeliveryState = "failed"
)

// Delivery is a notification's progress on one channel.
type Delivery struct {
	Channel     string
	State       DeliveryState
	Attempts    int
	LastError   string
	DeliveredAt time.Time
}

// Notification is a message for one account about one transfer event.
type Notification struct {
	ID         uuid.UUID
	EventID    uuid.UUID
	AccountID  uuid.UUID
	TransferID uuid.UUID
	Kind       Kind
	Message    string
	CreatedAt  time.Time
	Deliveries []Delivery
}

// Delivery returns the delivery on channel.
func (n Notification) Delivery(channel string) (Delivery, bool) {
	for _, d := range n.Deliveries {
		if d.Channel == channel {
			return d, true
		}
	}
	return Delivery{}, false
}

// ForEvent returns the notifications an event produces, at most one per
// account. transfer.held tells nobody anything new.
func ForEvent(e eventsv1.TransferEvent, newID func() (uuid.UUID, error), now time.Time) ([]Notification, error) {
	t := e.Transfer
	amount := FormatMinor(t.AmountMinor, t.Currency)
	type note struct {
		account uuid.UUID
		kind    Kind
		message string
	}
	var notes []note
	switch e.Type {
	case eventsv1.TransferCreated:
		notes = append(notes, note{t.SourceAccountID, KindAccepted,
			fmt.Sprintf("Transfer of %s to account %s accepted.", amount, short(t.DestinationAccountID))})
	case eventsv1.TransferCompleted:
		notes = append(notes,
			note{t.SourceAccountID, KindSent, fmt.Sprintf("You sent %s to account %s.", amount, short(t.DestinationAccountID))},
			note{t.DestinationAccountID, KindReceived, fmt.Sprintf("You received %s from account %s.", amount, short(t.SourceAccountID))})
	case eventsv1.TransferRejected:
		notes = append(notes, note{t.SourceAccountID, KindRejected,
			fmt.Sprintf("Transfer of %s was rejected: %s.", amount, reasonText(t.Reason))})
	case eventsv1.TransferCanceled:
		notes = append(notes, note{t.SourceAccountID, KindCanceled,
			fmt.Sprintf("Transfer of %s was canceled: %s. The money is available again.", amount, reasonText(t.Reason))})
	}
	out := make([]Notification, 0, len(notes))
	for _, n := range notes {
		id, err := newID()
		if err != nil {
			return nil, fmt.Errorf("generate notification id: %w", err)
		}
		out = append(out, Notification{
			ID: id, EventID: e.ID, AccountID: n.account, TransferID: t.ID,
			Kind: n.kind, Message: n.message, CreatedAt: now,
		})
	}
	return out, nil
}

var reasons = map[string]string{
	"insufficient_funds":       "insufficient funds",
	"account_not_found":        "unknown account",
	"currency_mismatch":        "the accounts use other currencies",
	"screening_declined":       "declined by screening",
	"screening_unavailable":    "screening was unavailable",
	"screening_review_timeout": "screening took too long",
	"hold_expired":             "the reservation expired",
}

// reasonText turns a reason code into words; an unknown code, added by the
// producer later, still reads.
func reasonText(code string) string {
	if text, ok := reasons[code]; ok {
		return text
	}
	if code == "" {
		return "no reason given"
	}
	return strings.ReplaceAll(code, "_", " ")
}

// short is the beginning of an id, enough to recognise an account.
func short(id uuid.UUID) string { return id.String()[:8] }

var exponents = map[string]int{"JPY": 0}

// FormatMinor renders an amount in minor units with its currency, for
// example 1250 EUR as "12.50 EUR". Currencies have two decimals unless
// listed otherwise; integer arithmetic only.
func FormatMinor(amountMinor int64, currency string) string {
	exp, ok := exponents[currency]
	if !ok {
		exp = 2
	}
	sign := ""
	if amountMinor < 0 {
		sign, amountMinor = "-", -amountMinor
	}
	digits := strconv.FormatInt(amountMinor, 10)
	if exp == 0 {
		return sign + digits + " " + currency
	}
	if len(digits) <= exp {
		digits = strings.Repeat("0", exp-len(digits)+1) + digits
	}
	return sign + digits[:len(digits)-exp] + "." + digits[len(digits)-exp:] + " " + currency
}
