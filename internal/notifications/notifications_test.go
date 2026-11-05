package notifications

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
)

func TestFormatMinor(t *testing.T) {
	for _, tt := range []struct {
		amount   int64
		currency string
		want     string
	}{
		{1250, "EUR", "12.50 EUR"},
		{5, "EUR", "0.05 EUR"},
		{100, "USD", "1.00 USD"},
		{1250, "JPY", "1250 JPY"},
		{-75, "GBP", "-0.75 GBP"},
	} {
		if got := FormatMinor(tt.amount, tt.currency); got != tt.want {
			t.Errorf("FormatMinor(%d, %s) = %q, want %q", tt.amount, tt.currency, got, tt.want)
		}
	}
}

func event(typ eventsv1.Type, reason string) eventsv1.TransferEvent {
	return eventsv1.TransferEvent{
		ID: uuid.New(), Type: typ, SchemaVersion: 1, OccurredAt: time.Now(),
		Transfer: eventsv1.Transfer{ID: uuid.New(), SourceAccountID: uuid.New(), DestinationAccountID: uuid.New(),
			AmountMinor: 1250, Currency: "EUR", Status: "x", Version: 2, Reason: reason},
	}
}

func TestForEvent(t *testing.T) {
	tests := []struct {
		typ      eventsv1.Type
		reason   string
		kinds    []Kind
		contains string
	}{
		{eventsv1.TransferCreated, "", []Kind{KindAccepted}, "accepted"},
		{eventsv1.TransferHeld, "", nil, ""},
		{eventsv1.TransferCompleted, "", []Kind{KindSent, KindReceived}, "12.50 EUR"},
		{eventsv1.TransferRejected, "insufficient_funds", []Kind{KindRejected}, "insufficient funds"},
		{eventsv1.TransferCanceled, "brand_new_reason", []Kind{KindCanceled}, "brand new reason"},
	}
	for _, tt := range tests {
		e := event(tt.typ, tt.reason)
		got, err := ForEvent(e, uuid.NewV7, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(tt.kinds) {
			t.Fatalf("%s: %d notifications, want %d", tt.typ, len(got), len(tt.kinds))
		}
		for i, n := range got {
			if n.Kind != tt.kinds[i] || n.EventID != e.ID || n.TransferID != e.Transfer.ID || !strings.Contains(n.Message, tt.contains) {
				t.Errorf("%s: %+v", tt.typ, n)
			}
		}
		if tt.typ == eventsv1.TransferCompleted && (got[0].AccountID != e.Transfer.SourceAccountID || got[1].AccountID != e.Transfer.DestinationAccountID) {
			t.Errorf("completed: sent and received go to the source and the destination")
		}
	}
}
