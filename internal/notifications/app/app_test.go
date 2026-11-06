package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
)

// memStore is an in-memory Store, Enqueuer and TxRunner with rollback.
type memStore struct {
	mu        sync.Mutex
	processed map[uuid.UUID]bool
	notes     map[uuid.UUID]notifications.Notification
	tasks     []taskqueue.Spec
	failNext  error
}

func newMemStore() *memStore {
	return &memStore{processed: map[uuid.UUID]bool{}, notes: map[uuid.UUID]notifications.Notification{}}
}

func (s *memStore) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	snapProcessed, snapNotes, snapTasks := map[uuid.UUID]bool{}, map[uuid.UUID]notifications.Notification{}, slices.Clone(s.tasks)
	for k, v := range s.processed {
		snapProcessed[k] = v
	}
	for k, v := range s.notes {
		snapNotes[k] = v
	}
	s.mu.Unlock()
	if err := fn(ctx); err != nil {
		s.mu.Lock()
		s.processed, s.notes, s.tasks = snapProcessed, snapNotes, snapTasks
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *memStore) MarkProcessed(_ context.Context, e ProcessedEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failNext; err != nil {
		s.failNext = nil
		return false, err
	}
	if s.processed[e.EventID] {
		return false, nil
	}
	s.processed[e.EventID] = true
	return true, nil
}

func (s *memStore) InsertNotification(_ context.Context, n notifications.Notification, channels []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range channels {
		n.Deliveries = append(n.Deliveries, notifications.Delivery{Channel: ch, State: notifications.DeliveryPending})
	}
	s.notes[n.ID] = n
	return nil
}

func (s *memStore) Notification(_ context.Context, id uuid.UUID) (notifications.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notes[id]
	if !ok {
		return notifications.Notification{}, notifications.ErrNotFound
	}
	n.Deliveries = slices.Clone(n.Deliveries)
	return n, nil
}

func (s *memStore) RecordAttempt(_ context.Context, id uuid.UUID, channel string, a Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notes[id]
	for i := range n.Deliveries {
		d := &n.Deliveries[i]
		if d.Channel != channel {
			continue
		}
		d.Attempts = a.Number
		switch {
		case a.GiveUp:
			d.State = notifications.DeliveryFailed
		case a.Err == nil:
			d.State = notifications.DeliveryDelivered
		}
	}
	s.notes[id] = n
	return nil
}

func (s *memStore) ByAccount(_ context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []notifications.Notification
	for _, n := range s.notes {
		if n.AccountID == account && len(out) < limit {
			out = append(out, n)
		}
	}
	return out, nil
}

func (s *memStore) Enqueue(_ context.Context, spec taskqueue.Spec) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, spec)
	return true, nil
}

type flakyChannel struct {
	name  string
	fails int
	sent  int
}

func (c *flakyChannel) Name() string { return c.name }

func (c *flakyChannel) Send(context.Context, notifications.Notification) error {
	if c.fails > 0 {
		c.fails--
		return errors.New("receiver down")
	}
	c.sent++
	return nil
}

type nopMetrics struct{ created, gaveUp int }

func (m *nopMetrics) NotificationCreated(notifications.Kind) { m.created++ }
func (m *nopMetrics) Delivered(string, bool)                 {}
func (m *nopMetrics) GaveUp(string)                          { m.gaveUp++ }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Unix(1_000, 0) }

func newService(store *memStore, channels ...Channel) (*Service, *nopMetrics) {
	m := &nopMetrics{}
	return NewService(Deps{
		Tx: store, Store: store, Tasks: store, Channels: channels, Clock: fixedClock{}, Metrics: m,
		NewID: uuid.NewV7, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, Config{DeliveryMaxAttempts: 3}), m
}

func completed() (eventsv1.TransferEvent, Message) {
	e := eventsv1.TransferEvent{
		ID: uuid.New(), Type: eventsv1.TransferCompleted, SchemaVersion: 1, OccurredAt: time.Unix(5, 0).UTC(),
		Transfer: eventsv1.Transfer{ID: uuid.New(), SourceAccountID: uuid.New(), DestinationAccountID: uuid.New(),
			AmountMinor: 1250, Currency: "EUR", Status: "completed", Version: 7},
	}
	body, _ := eventsv1.Encode(e)
	return e, Message{Topic: eventsv1.TopicTransfers, Key: []byte(e.Transfer.ID.String()), Value: body, EventID: e.ID.String()}
}

// The same event twice creates its notifications and deliveries once.
func TestDuplicateEventCreatesOneSetOfNotifications(t *testing.T) {
	store := newMemStore()
	svc, m := newService(store, &flakyChannel{name: "log"}, &flakyChannel{name: "webhook"})
	_, msg := completed()

	if out, err := svc.HandleEvent(context.Background(), msg); err != nil || out != OutcomeStored {
		t.Fatalf("first = %q, %v", out, err)
	}
	if out, err := svc.HandleEvent(context.Background(), msg); err != nil || out != OutcomeDuplicate {
		t.Fatalf("second = %q, %v", out, err)
	}
	if len(store.notes) != 2 || len(store.tasks) != 4 || m.created != 2 {
		t.Fatalf("notes %d, delivery tasks %d, counted %d; want 2, 4 and 2", len(store.notes), len(store.tasks), m.created)
	}
}

// A failed transaction leaves no trace, so the redelivery is processed in
// full, not taken for a duplicate.
func TestFailureLeavesNoTrace(t *testing.T) {
	store := newMemStore()
	svc, _ := newService(store, &flakyChannel{name: "log"})
	_, msg := completed()
	store.failNext = errors.New("database unavailable")
	if _, err := svc.HandleEvent(context.Background(), msg); err == nil || errors.Is(err, notifications.ErrInvalidEvent) {
		t.Fatalf("err = %v, want a transient error", err)
	}
	if out, err := svc.HandleEvent(context.Background(), msg); err != nil || out != OutcomeStored {
		t.Fatalf("redelivery = %q, %v", out, err)
	}
}

// A type the contract does not list yet is skipped and committed: a
// producer may add one before this consumer knows it.
func TestUnknownTypeIsIgnored(t *testing.T) {
	store := newMemStore()
	svc, m := newService(store, &flakyChannel{name: "log"})
	e, msg := completed()
	e.Type = "transfer.refunded"
	msg.Value, _ = json.Marshal(e)
	if out, err := svc.HandleEvent(context.Background(), msg); err != nil || out != OutcomeIgnored {
		t.Fatalf("HandleEvent = %q, %v; want ignored", out, err)
	}
	if len(store.notes) != 0 || len(store.tasks) != 0 || m.created != 0 {
		t.Fatalf("an ignored event stored %d notifications and %d tasks", len(store.notes), len(store.tasks))
	}
}

// A channel given twice would break the deliveries key on every event.
func TestNewServiceRefusesARepeatedChannel(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewService accepted the log channel twice")
		}
	}()
	newService(newMemStore(), &flakyChannel{name: "log"}, &flakyChannel{name: "log"})
}

func TestPoisonIsInvalidEvent(t *testing.T) {
	svc, _ := newService(newMemStore(), &flakyChannel{name: "log"})
	e, good := completed()
	wrongKey := good
	wrongKey.Key = []byte("someone-else")
	wrongHeader := good
	wrongHeader.EventID = uuid.NewString()
	future := e
	future.SchemaVersion = 2
	futureBody, _ := json.Marshal(future)
	toItself := e
	toItself.Transfer.DestinationAccountID = toItself.Transfer.SourceAccountID
	toItselfBody, _ := json.Marshal(toItself)
	for name, m := range map[string]Message{
		"not JSON":      {Value: []byte("pony")},
		"wrong key":     wrongKey,
		"wrong header":  wrongHeader,
		"future schema": {Value: futureBody},
		"to itself":     {Value: toItselfBody},
	} {
		if _, err := svc.HandleEvent(context.Background(), m); !errors.Is(err, notifications.ErrInvalidEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidEvent", name, err)
		}
	}
}

func TestDeliveryRetriesThenGivesUp(t *testing.T) {
	store := newMemStore()
	ch := &flakyChannel{name: "webhook", fails: 100}
	svc, m := newService(store, ch)
	_, msg := completed()
	if _, err := svc.HandleEvent(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	task := taskqueue.Task{Kind: KindDeliver, AggregateID: store.tasks[0].AggregateID, Payload: store.tasks[0].Payload}
	for failures := range 3 {
		task.Attempts = failures
		if err := svc.HandleTask(context.Background(), task); err == nil {
			t.Fatalf("attempt %d: want the send error for a retry", failures+1)
		}
	}
	task.Attempts = 3
	if err := svc.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("after the last attempt the task ends: %v", err)
	}
	n, _ := store.Notification(context.Background(), task.AggregateID)
	if d, _ := n.Delivery("webhook"); d.State != notifications.DeliveryFailed || m.gaveUp != 1 {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestDeliveryIsDoneOnce(t *testing.T) {
	store := newMemStore()
	ch := &flakyChannel{name: "webhook", fails: 1}
	svc, _ := newService(store, ch)
	_, msg := completed()
	if _, err := svc.HandleEvent(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	task := taskqueue.Task{Kind: KindDeliver, AggregateID: store.tasks[0].AggregateID, Payload: store.tasks[0].Payload}
	if err := svc.HandleTask(context.Background(), task); err == nil {
		t.Fatal("first try fails")
	}
	task.Attempts = 1
	for range 2 { // the second run is a lease takeover: nothing left to do
		if err := svc.HandleTask(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	if ch.sent != 1 {
		t.Fatalf("sent %d times, want 1", ch.sent)
	}
}
