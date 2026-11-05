// Package kafkapub publishes outbox events to the transfers.events topic
// with franz-go. It implements outbox.Publisher.
//
// Each event is one record: the key is the transfer id, so all events of a
// transfer land on one partition and stay in order; the value is the
// eventsv1 JSON body; headers carry the event id and type, so a consumer can
// deduplicate or route without parsing the body.
//
// The producer is idempotent (franz-go's default) and waits for all
// in-sync replicas: an acknowledged record survives a broker failure, and a
// retry inside the client does not write a duplicate. A record that times
// out may still have been written; the relay then publishes it again, and
// the consumer's deduplication by event id absorbs it.
package kafkapub

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/outbox"
)

// Producer is the part of *kgo.Client the publisher needs.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Publisher writes outbox records to one topic.
type Publisher struct {
	producer Producer
	topic    string
}

// New returns a Publisher to topic.
func New(p Producer, topic string) *Publisher {
	return &Publisher{producer: p, topic: topic}
}

// Publish writes r and waits for the broker's acknowledgement or ctx.
func (p *Publisher) Publish(ctx context.Context, r outbox.Record) error {
	// Timestamp stays zero, so the client sets it to now. franz-go measures
	// RecordDeliveryTimeout from the record's timestamp: an event created
	// before a broker outage would otherwise time out the moment it is
	// produced, and the relay would never get past it. When the event
	// happened is in the body (occurred_at).
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(r.AggregateID.String()),
		Value: r.Payload,
		Headers: []kgo.RecordHeader{
			{Key: eventsv1.HeaderEventID, Value: []byte(r.EventID.String())},
			{Key: eventsv1.HeaderEventType, Value: []byte(r.Type)},
			{Key: eventsv1.HeaderContentType, Value: []byte(eventsv1.ContentType)},
		},
	}
	if err := p.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", p.topic, err)
	}
	return nil
}

// ProducerOptions are the client options of the relay's producer.
// deliveryTimeout bounds how long a record may wait in the client; it
// should not exceed the relay's publish timeout.
func ProducerOptions(deliveryTimeout time.Duration) []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// The relay sends one record and waits: lingering for a batch would
		// only add latency.
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		// Hash the key, as other Kafka clients do, so every producer puts a
		// transfer on the same partition.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
}
