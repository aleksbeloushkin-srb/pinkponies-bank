//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/channels"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/consumer"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/httpapi"
	notificationsmetrics "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/metrics"
	notificationspg "github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications/postgres"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/kafka"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/kafkapub"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/pkg/txctx"
)

// notificationsEnv is the notifications service on its database, reading a
// topic of its own as a consumer group of its own.
type notificationsEnv struct {
	topic, dlq, group string
	svc               *app.Service
	tasks             *taskqueue.Postgres
	sink              *httpapi.Sink
}

func newNotificationsEnv(t *testing.T) *notificationsEnv {
	t.Helper()
	if _, err := notificationsDB.Exec(context.Background(),
		`TRUNCATE delivery_attempts, deliveries, notifications, processed_events, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	e := &notificationsEnv{topic: newTopic(t, 3), dlq: newTopic(t, 1), group: "g-" + uuid.NewString()[:8]}
	sink, err := httpapi.NewSink(nil)
	if err != nil {
		t.Fatal(err)
	}
	e.sink = sink
	sinkSrv := httptest.NewServer(httpapi.NewHandler(httpapi.Deps{Lister: nil, Sink: sink,
		Metrics: httpserverMetrics(), Logger: discard()}))
	t.Cleanup(func() {
		sink.Close()
		sinkSrv.Close()
	})
	webhook, err := channels.NewWebhook(sinkSrv.URL+"/_sink/webhooks", sinkSrv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tx := txctx.New(notificationsDB)
	e.tasks = taskqueue.NewPostgres(tx)
	e.svc = app.NewService(app.Deps{
		Tx: tx, Store: notificationspg.NewStore(tx), Tasks: e.tasks,
		Channels: []app.Channel{channels.NewLog(discard()), webhook}, Clock: newClock(),
		Metrics: notificationsmetrics.NewBusiness(prometheus.NewRegistry(), []string{"log", "webhook"}),
		NewID:   uuid.NewV7, Logger: discard(),
	}, app.Config{DeliveryMaxAttempts: 3})
	return e
}

// signalling counts handled messages on a channel, so a test waits for
// the consumer by events rather than by sleeping.
type signalling struct {
	inner   consumer.Handler
	handled chan string
}

func (s signalling) HandleEvent(ctx context.Context, m app.Message) (string, error) {
	out, err := s.inner.HandleEvent(ctx, m)
	if err == nil {
		s.handled <- out
	}
	return out, err
}

// consume runs a consumer group member over the env's topic until n
// messages were handled (stored, duplicate or dead-lettered handled by the
// wrapped handler), then stops it, which commits what it handled.
func (e *notificationsEnv) consume(t *testing.T, h consumer.Handler, n int) []string {
	t.Helper()
	cl := newKafkaClient(t, consumer.Options(e.group, e.topic)...)
	handled := make(chan string, 1000)
	c := consumer.New(cl, consumer.NewDeadLetterTopic(cl, e.dlq), signalling{inner: h, handled: handled},
		consumer.NewMetrics(prometheus.NewRegistry()), consumer.Config{
			MaxPollRecords: 100, MaxAttempts: 3,
			Backoff:       taskqueue.Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
			HandleTimeout: 5 * time.Second, CommitTimeout: 5 * time.Second,
		}, discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.Run(ctx) }()
	var outcomes []string
	deadline := time.After(60 * time.Second)
	for len(outcomes) < n {
		select {
		case o := <-handled:
			outcomes = append(outcomes, o)
		case <-deadline:
			cancel()
			t.Fatalf("handled %d of %d messages", len(outcomes), n)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cl.Close()
	return outcomes
}

// lag counts the records of the topic the group has not committed, from
// the group's committed offsets and the topic's end offsets. (kadm's Lag
// reports nothing for a group that never committed, which would hide
// exactly the case under test.)
func (e *notificationsEnv) lag(t *testing.T) int64 {
	t.Helper()
	ctx := context.Background()
	adm := kadm.NewClient(newKafkaClient(t))
	committed, err := adm.FetchOffsets(ctx, e.group)
	if err != nil {
		t.Fatal(err)
	}
	end, err := adm.ListEndOffsets(ctx, e.topic)
	if err != nil {
		t.Fatal(err)
	}
	var lag int64
	end.Each(func(o kadm.ListedOffset) {
		at := int64(0)
		if c, ok := committed.Lookup(o.Topic, o.Partition); ok && c.Err == nil && c.At >= 0 {
			at = c.At
		}
		lag += o.Offset - at
	})
	return lag
}

func (e *notificationsEnv) produce(t *testing.T, recs ...*kgo.Record) {
	t.Helper()
	cl := newKafkaClient(t, kafkapub.ProducerOptions(5*time.Second)...)
	for _, r := range recs {
		r.Topic = e.topic
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func eventRecord(t *testing.T, e eventsv1.TransferEvent) *kgo.Record {
	t.Helper()
	body, err := eventsv1.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Key: []byte(e.Transfer.ID.String()), Value: body,
		Headers: []kgo.RecordHeader{{Key: eventsv1.HeaderEventID, Value: []byte(e.ID.String())}}}
}

func completedEvent() eventsv1.TransferEvent {
	return eventsv1.TransferEvent{
		ID: uuid.New(), Type: eventsv1.TransferCompleted, SchemaVersion: 1, OccurredAt: time.Now().UTC(),
		Transfer: eventsv1.Transfer{ID: uuid.New(), SourceAccountID: uuid.New(), DestinationAccountID: uuid.New(),
			AmountMinor: 1250, Currency: "EUR", Status: "completed", Version: 7},
	}
}

// The same event published twice is handled twice and stored once; both
// offsets are committed.
func TestConsumerDuplicateEventOneNotification(t *testing.T) {
	e := newNotificationsEnv(t)
	ev := completedEvent()
	e.produce(t, eventRecord(t, ev), eventRecord(t, ev))

	outcomes := e.consume(t, e.svc, 2)
	if outcomes[0] != app.OutcomeStored || outcomes[1] != app.OutcomeDuplicate {
		t.Fatalf("outcomes = %v", outcomes)
	}
	if n := count(t, notificationsDB, `SELECT count(*) FROM notifications WHERE event_id = $1`, ev.ID); n != 2 {
		t.Fatalf("notifications = %d, want 2 (sender and receiver), once", n)
	}
	if n := count(t, notificationsDB, `SELECT count(*) FROM tasks WHERE kind = 'deliver'`); n != 4 {
		t.Fatalf("delivery tasks = %d, want 4", n)
	}
	if lag := e.lag(t); lag != 0 {
		t.Fatalf("lag = %d, want 0", lag)
	}
}

// A poison message is retried, then written to the dead-letter topic with
// its reasons, and the consumer carries on with the next record.
func TestConsumerPoisonGoesToTheDLQ(t *testing.T) {
	e := newNotificationsEnv(t)
	ev := completedEvent()
	e.produce(t, &kgo.Record{Key: []byte(ev.Transfer.ID.String()), Value: []byte("this is not an event")}, eventRecord(t, ev))

	// Both records go to one partition (same key): the poison first.
	var poisonSeen int
	h := handlerFunc2(func(ctx context.Context, m app.Message) (string, error) {
		out, err := e.svc.HandleEvent(ctx, m)
		if errors.Is(err, notifications.ErrInvalidEvent) {
			poisonSeen++
		}
		return out, err
	})
	if out := e.consume(t, h, 1); out[0] != app.OutcomeStored {
		t.Fatalf("outcomes = %v", out)
	}
	if poisonSeen != 3 {
		t.Fatalf("poison tried %d times, want 3", poisonSeen)
	}
	dlq := readAll(t, e.dlq, 1)
	if string(dlq[0].Value) != "this is not an event" || header(dlq[0], consumer.HeaderDLQAttempts) != "3" ||
		header(dlq[0], consumer.HeaderDLQOriginalTopic) != e.topic || header(dlq[0], consumer.HeaderDLQError) == "" {
		t.Fatalf("dead letter %s headers %v", dlq[0].Value, dlq[0].Headers)
	}
	if lag := e.lag(t); lag != 0 {
		t.Fatalf("lag = %d: the dead-lettered record must be committed too", lag)
	}
	lag, dlqSize := e.collect(t)
	if lag != 0 || dlqSize != 1 {
		t.Fatalf("collector: lag %v, dlq messages %v; want 0 and 1", lag, dlqSize)
	}
}

// An event that keeps the contract but that the database refuses (a NUL
// byte in the reason, which a PostgreSQL text column cannot hold) is poison
// too: it is dead-lettered after its retries instead of stopping its
// partition, and the record behind it on the same partition is handled.
func TestConsumerStoreRefusalGoesToTheDLQ(t *testing.T) {
	e := newNotificationsEnv(t)
	bad := completedEvent()
	bad.Type, bad.Transfer.Status, bad.Transfer.Reason = eventsv1.TransferRejected, "rejected", "insufficient\x00funds"
	good := completedEvent()
	good.Transfer.ID = bad.Transfer.ID // same key: same partition, behind the bad one
	badRec := eventRecord(t, bad)
	e.produce(t, badRec, eventRecord(t, good))

	var refused int
	h := handlerFunc2(func(ctx context.Context, m app.Message) (string, error) {
		out, err := e.svc.HandleEvent(ctx, m)
		if errors.Is(err, notifications.ErrEventRefused) {
			refused++
		}
		return out, err
	})
	if out := e.consume(t, h, 1); out[0] != app.OutcomeStored {
		t.Fatalf("outcomes = %v", out)
	}
	if refused != 3 {
		t.Fatalf("refused event tried %d times, want 3", refused)
	}
	if dlq := readAll(t, e.dlq, 1); string(dlq[0].Value) != string(badRec.Value) {
		t.Fatalf("dead letter %s, want the refused event", dlq[0].Value)
	}
	if lag := e.lag(t); lag != 0 {
		t.Fatalf("lag = %d, want 0", lag)
	}
}

// The store marks a refusal of the data itself as poison, and a failure
// that may pass on a retry as transient.
func TestNotificationsStoreClassifiesRefusals(t *testing.T) {
	newNotificationsEnv(t)
	ctx := context.Background()
	tx := txctx.New(notificationsDB)
	store := notificationspg.NewStore(tx)
	ev := completedEvent()
	processed := app.ProcessedEvent{EventID: ev.ID, TransferID: ev.Transfer.ID, Type: string(ev.Type), Topic: "t"}
	err := tx.Run(ctx, func(ctx context.Context) error {
		if _, err := store.MarkProcessed(ctx, processed); err != nil {
			return err
		}
		n := notifications.Notification{ID: uuid.New(), EventID: ev.ID, AccountID: uuid.New(), TransferID: ev.Transfer.ID,
			Kind: notifications.KindSent, Message: "sent", CreatedAt: time.Now()}
		return store.InsertNotification(ctx, n, []string{"log", "log"}) // the deliveries key, twice
	})
	if !errors.Is(err, notifications.ErrEventRefused) || !errors.Is(err, notifications.ErrInvalidEvent) {
		t.Fatalf("a constraint violation = %v, want poison", err)
	}
	gone, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.MarkProcessed(gone, processed); err == nil || errors.Is(err, notifications.ErrInvalidEvent) {
		t.Fatalf("a cancelled statement = %v, want a transient error", err)
	}
}

// collect scrapes the Kafka collector of the service for the env's group,
// topic and dead-letter topic.
func (e *notificationsEnv) collect(t *testing.T) (lag, dlqMessages float64) {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(kafka.NewCollector(kadm.NewClient(newKafkaClient(t)), e.group, []string{e.topic}, []string{e.dlq}, 10*time.Second, discard()))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			switch mf.GetName() {
			case "kafka_consumer_group_lag":
				lag, found = m.GetGauge().GetValue(), found+1
			case "kafka_topic_messages":
				dlqMessages, found = m.GetGauge().GetValue(), found+1
			}
		}
	}
	if found != 2 {
		t.Fatalf("collector exported %d of 2 gauges", found)
	}
	return lag, dlqMessages
}

type handlerFunc2 func(ctx context.Context, m app.Message) (string, error)

func (f handlerFunc2) HandleEvent(ctx context.Context, m app.Message) (string, error) {
	return f(ctx, m)
}

// While storing fails, the offset is not committed: a consumer that stops
// during the failure leaves the record to the next member of the group,
// which stores it.
func TestConsumerOffsetNotCommittedOnFailure(t *testing.T) {
	e := newNotificationsEnv(t)
	ev := completedEvent()
	e.produce(t, eventRecord(t, ev))

	tried := make(chan struct{}, 100)
	failing := handlerFunc2(func(context.Context, app.Message) (string, error) {
		tried <- struct{}{}
		return "", errors.New("database unavailable")
	})
	cl := newKafkaClient(t, consumer.Options(e.group, e.topic)...)
	c := consumer.New(cl, consumer.NewDeadLetterTopic(cl, e.dlq), failing, consumer.NewMetrics(prometheus.NewRegistry()),
		consumer.Config{MaxPollRecords: 10, MaxAttempts: 3, HandleTimeout: time.Second, CommitTimeout: time.Second,
			Backoff: taskqueue.Backoff{Base: 10 * time.Millisecond, Max: 20 * time.Millisecond}}, discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.Run(ctx) }()
	for range 5 { // more tries than MaxAttempts: a transient failure is never dead-lettered
		<-tried
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cl.Close()
	if lag := e.lag(t); lag != 1 {
		t.Fatalf("lag = %d, want 1: nothing may be committed", lag)
	}
	if lag, _ := e.collect(t); lag != 1 {
		t.Fatalf("collector lag = %v with the consumer down, want 1", lag)
	}

	if out := e.consume(t, e.svc, 1); out[0] != app.OutcomeStored {
		t.Fatalf("redelivered outcome = %v", out)
	}
	if n := count(t, notificationsDB, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Fatalf("processed = %d", n)
	}
	if lag := e.lag(t); lag != 0 {
		t.Fatalf("lag = %d after the redelivery", lag)
	}
}

// The whole pipeline: a transfer completes, the relay publishes its events,
// the relay publishes them again (as after a crash before marking), the
// consumer stores each event once, and the webhook receives each
// notification once.
func TestPipelineTransferToNotificationsDeliveredOnce(t *testing.T) {
	s := newSagaEnv(t, sagaOptions{})
	n := newNotificationsEnv(t)
	s.create(t, 1250)
	s.drain(t)

	cl := newKafkaClient(t, kafkapub.ProducerOptions(5*time.Second)...)
	pub := kafkapub.New(cl, n.topic)
	if got, err := newRelay(s, pub, s.outbox).RunOnce(context.Background()); err != nil || got != 3 {
		t.Fatalf("published %d, %v", got, err)
	}
	if _, err := transfersDB.Exec(context.Background(), `UPDATE outbox SET published_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if got, err := newRelay(s, pub, s.outbox).RunOnce(context.Background()); err != nil || got != 3 {
		t.Fatalf("republished %d, %v", got, err)
	}

	outcomes := n.consume(t, n.svc, 6)
	stored, dup := 0, 0
	for _, o := range outcomes {
		switch o {
		case app.OutcomeStored:
			stored++
		case app.OutcomeDuplicate:
			dup++
		}
	}
	if stored != 3 || dup != 3 {
		t.Fatalf("outcomes = %v, want 3 stored and 3 duplicates", outcomes)
	}
	// created -> 1 (source), held -> 0, completed -> 2 (source, destination)
	if got := count(t, notificationsDB, `SELECT count(*) FROM notifications`); got != 3 {
		t.Fatalf("notifications = %d, want 3", got)
	}

	w := taskqueue.NewWorker(n.tasks, n.svc, newClock(), nil, taskqueue.NewMetrics(prometheus.NewRegistry(), app.TaskKinds), discard(),
		taskqueue.Config{Concurrency: 8, IdleInterval: time.Hour, Lease: time.Minute, TaskTimeout: 5 * time.Second, MaxAttempts: 5,
			Backoff: taskqueue.Backoff{Base: time.Millisecond, Max: time.Millisecond}})
	for {
		got, err := w.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			break
		}
	}
	if st := n.sink.Stats(); st.Unique != 3 || st.Duplicates != 0 {
		t.Fatalf("sink = %+v, want 3 notifications delivered once each", st)
	}
	if got := count(t, notificationsDB, `SELECT count(*) FROM deliveries WHERE state = 'delivered'`); got != 6 {
		t.Fatalf("delivered = %d, want 6 (3 notifications x 2 channels)", got)
	}
}
