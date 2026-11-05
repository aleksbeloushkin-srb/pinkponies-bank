//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/kafkapub"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/outbox"
)

func newRelay(e *sagaEnv, pub outbox.Publisher, store outbox.Store) *outbox.Relay {
	return outbox.NewRelay(store, pub, e.clock, nil, outbox.NewMetrics(prometheus.NewRegistry()),
		outbox.Config{Interval: time.Hour, Batch: 50, Lease: time.Minute, PublishTimeout: 5 * time.Second}, discard())
}

// The saga's events go through the outbox and the relay to Kafka: one
// record per status change, keyed by transfer id, in order per transfer,
// with the event id and type in headers and a body that decodes.
func TestRelayPublishesTheSagaToKafka(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	topic := newTopic(t, 3)
	ids := []uuid.UUID{e.createKey(t, "a", 1250), e.createKey(t, "b", 1266), e.createKey(t, "c", 10_001)}
	e.drain(t)

	cl := newKafkaClient(t, kafkapub.ProducerOptions(5*time.Second)...)
	relay := newRelay(e, kafkapub.New(cl, topic), e.outbox)
	n, err := relay.RunOnce(context.Background())
	if err != nil || n != 8 { // 3 + 3 + 2 events
		t.Fatalf("published %d, %v; want 8", n, err)
	}
	if left := count(t, transfersDB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); left != 0 {
		t.Fatalf("%d events left unpublished", left)
	}

	records := readAll(t, topic, 8)
	byTransfer := map[uuid.UUID][]string{}
	partitionOf := map[uuid.UUID]int32{}
	for _, r := range records {
		ev, err := eventsv1.Decode(r.Value)
		if err != nil {
			t.Fatalf("record %s: %v", r.Value, err)
		}
		if string(r.Key) != ev.Transfer.ID.String() || header(r, eventsv1.HeaderEventID) != ev.ID.String() ||
			header(r, eventsv1.HeaderEventType) != string(ev.Type) {
			t.Fatalf("key %s, headers %v for event %+v", r.Key, r.Headers, ev)
		}
		if p, seen := partitionOf[ev.Transfer.ID]; seen && p != r.Partition {
			t.Fatalf("transfer %s on partitions %d and %d", ev.Transfer.ID, p, r.Partition)
		}
		partitionOf[ev.Transfer.ID] = r.Partition
		byTransfer[ev.Transfer.ID] = append(byTransfer[ev.Transfer.ID], string(ev.Type))
	}
	want := map[uuid.UUID][]string{
		ids[0]: {"transfer.created", "transfer.held", "transfer.completed"},
		ids[1]: {"transfer.created", "transfer.held", "transfer.rejected"},
		ids[2]: {"transfer.created", "transfer.rejected"},
	}
	for id, types := range want {
		if got := byTransfer[id]; len(got) != len(types) {
			t.Fatalf("transfer %s: %v, want %v", id, got, types)
		}
		for i := range types {
			if byTransfer[id][i] != types[i] {
				t.Fatalf("transfer %s out of order: %v, want %v", id, byTransfer[id], types)
			}
		}
	}
}

// failingMark publishes for real but fails to mark the events, like a relay
// that crashed after Kafka acknowledged and before its UPDATE.
type failingMark struct{ outbox.Store }

func (failingMark) MarkPublished(context.Context, []int64, time.Time) error {
	return errors.New("connection lost before the mark")
}

// Delivery is at least once: an event published but not marked is
// published again when its lease expires, under the same event id.
func TestRelayRepublishesUnmarkedEvents(t *testing.T) {
	e := newSagaEnv(t, sagaOptions{})
	topic := newTopic(t, 1)
	e.create(t, 1250) // one transfer.created event
	cl := newKafkaClient(t, kafkapub.ProducerOptions(5*time.Second)...)
	pub := kafkapub.New(cl, topic)

	if _, err := newRelay(e, pub, failingMark{e.outbox}).RunOnce(context.Background()); err == nil {
		t.Fatal("want the mark error")
	}
	if n, _ := newRelay(e, pub, e.outbox).RunOnce(context.Background()); n != 0 {
		t.Fatal("the event is leased: nothing to publish until the lease expires")
	}
	e.clock.Advance(2 * time.Minute)
	if n, err := newRelay(e, pub, e.outbox).RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("after the lease: %d, %v", n, err)
	}
	records := readAll(t, topic, 2)
	if header(records[0], eventsv1.HeaderEventID) != header(records[1], eventsv1.HeaderEventID) {
		t.Fatal("a republished event must keep its id, so consumers can deduplicate it")
	}
}

func (e *sagaEnv) createKey(t *testing.T, key string, amount int64) uuid.UUID {
	t.Helper()
	res, err := e.svc.CreateTransfer(context.Background(), appCreate(e, key, amount))
	if err != nil {
		t.Fatal(err)
	}
	return res.Transfer.ID
}
