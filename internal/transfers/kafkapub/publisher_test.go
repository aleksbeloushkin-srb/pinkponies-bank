package kafkapub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/outbox"
)

type fakeProducer struct {
	got []*kgo.Record
	err error
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.got = append(f.got, rs...)
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r, Err: f.err}
	}
	return out
}

func TestPublishBuildsTheRecord(t *testing.T) {
	p := &fakeProducer{}
	rec := outbox.Record{ID: 1, EventID: uuid.New(), AggregateID: uuid.New(), Type: "transfer.held",
		Payload: []byte(`{"id":"x"}`), CreatedAt: time.Unix(100, 0)}
	if err := New(p, "transfers.events").Publish(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	got := p.got[0]
	if got.Topic != "transfers.events" || string(got.Key) != rec.AggregateID.String() || string(got.Value) != `{"id":"x"}` {
		t.Fatalf("record = %+v", got)
	}
	// franz-go times records out from their timestamp: an old event must not
	// carry its creation time, or it expires before it is sent.
	if !got.Timestamp.IsZero() {
		t.Fatalf("timestamp = %v, want zero so that the client stamps it", got.Timestamp)
	}
	headers := map[string]string{}
	for _, h := range got.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers[eventsv1.HeaderEventID] != rec.EventID.String() || headers[eventsv1.HeaderEventType] != "transfer.held" ||
		headers[eventsv1.HeaderContentType] != "application/json" {
		t.Fatalf("headers = %v", headers)
	}
}

func TestPublishReturnsTheBrokerError(t *testing.T) {
	boom := errors.New("not enough replicas")
	if err := New(&fakeProducer{err: boom}, "t").Publish(context.Background(), outbox.Record{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}
