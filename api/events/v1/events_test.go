package eventsv1

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func sample() TransferEvent {
	return TransferEvent{
		ID:            uuid.MustParse("0192f0a0-0000-7000-8000-000000000001"),
		Type:          TransferCompleted,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		Transfer: Transfer{
			ID:                   uuid.MustParse("0192f0a0-0000-7000-8000-000000000002"),
			SourceAccountID:      uuid.MustParse("0192f0a0-0000-7000-8000-000000000003"),
			DestinationAccountID: uuid.MustParse("0192f0a0-0000-7000-8000-000000000004"),
			AmountMinor:          1250,
			Currency:             "EUR",
			Status:               "completed",
			Version:              4,
		},
	}
}

// The wire format is part of the contract: this golden body must keep
// decoding to the same event.
func TestGoldenBody(t *testing.T) {
	const golden = `{"id":"0192f0a0-0000-7000-8000-000000000001","type":"transfer.completed","schema_version":1,` +
		`"occurred_at":"2026-05-01T09:00:00Z","transfer":{"id":"0192f0a0-0000-7000-8000-000000000002",` +
		`"source_account_id":"0192f0a0-0000-7000-8000-000000000003","destination_account_id":"0192f0a0-0000-7000-8000-000000000004",` +
		`"amount_minor":1250,"currency":"EUR","status":"completed","version":4}}`
	body, err := Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != golden {
		t.Fatalf("encoded:\n%s\nwant:\n%s", body, golden)
	}
	got, err := Decode([]byte(golden))
	if err != nil || got != sample() {
		t.Fatalf("Decode = %+v, %v", got, err)
	}
}

func TestDecodeIgnoresUnknownFields(t *testing.T) {
	body, _ := Encode(sample())
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	m["added_later"] = "fine"
	body, _ = json.Marshal(m)
	if _, err := Decode(body); err != nil {
		t.Fatalf("a new field must not break consumers: %v", err)
	}
}

func TestDecodeRejectsPoison(t *testing.T) {
	bad := func(mutate func(*TransferEvent)) []byte {
		e := sample()
		mutate(&e)
		b, _ := json.Marshal(e)
		return b
	}
	for name, body := range map[string][]byte{
		"not JSON":         []byte("hello, pony"),
		"no id":            bad(func(e *TransferEvent) { e.ID = uuid.Nil }),
		"unknown type":     bad(func(e *TransferEvent) { e.Type = "transfer.teleported" }),
		"no type":          bad(func(e *TransferEvent) { e.Type = "" }),
		"to itself":        bad(func(e *TransferEvent) { e.Transfer.DestinationAccountID = e.Transfer.SourceAccountID }),
		"future schema":    bad(func(e *TransferEvent) { e.SchemaVersion = 2 }),
		"negative amount":  bad(func(e *TransferEvent) { e.Transfer.AmountMinor = -1 }),
		"lowercase money":  bad(func(e *TransferEvent) { e.Transfer.Currency = "eur" }),
		"missing version":  bad(func(e *TransferEvent) { e.Transfer.Version = 0 }),
		"missing occurred": bad(func(e *TransferEvent) { e.OccurredAt = time.Time{} }),
	} {
		_, err := Decode(body)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
		// Only a type the contract does not list yet is skippable.
		if errors.Is(err, ErrUnknownType) != (name == "unknown type") {
			t.Errorf("%s: err = %v, ErrUnknownType = %v", name, err, errors.Is(err, ErrUnknownType))
		}
	}
}
