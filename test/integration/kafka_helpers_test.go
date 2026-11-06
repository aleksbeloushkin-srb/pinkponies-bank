//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/kafka"
)

// newTopic creates a topic with a name unique to the test and returns it.
func newTopic(t *testing.T, partitions int32) string {
	t.Helper()
	name := "t." + strings.NewReplacer("/", ".", " ", "_").Replace(t.Name()) + "." + uuid.NewString()[:8]
	cl := newKafkaClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kafka.EnsureTopics(ctx, cl, kafka.Topic{Name: name, Partitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	// The broker refuses new partitions past its per-core limit, which a
	// long -count run reaches if every run leaves its topics behind.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := kadm.NewClient(cl).DeleteTopic(ctx, name); err != nil {
			t.Logf("delete topic %s: %v", name, err)
		}
	})
	return name
}

func newKafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kafka.NewClient(kafka.Config{Brokers: kafkaBrokers, ClientID: "integration"}, discard(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// readAll consumes topic from the start until want records arrived or the
// deadline passed.
func readAll(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	cl := newKafkaClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records from %s before the deadline", len(out), want, topic)
		}
		fetches.EachError(func(_ string, _ int32, err error) { t.Fatalf("fetch: %v", err) })
		out = append(out, fetches.Records()...)
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
