// Package app implements the notifications use cases: turn a transfer
// event into notifications exactly once, deliver each notification on its
// channels, and list an account's notifications.
//
// Delivery from Kafka is at least once, so the same event can arrive
// twice. HandleEvent records the event id in processed_events in the same
// transaction as the notifications: a duplicate finds the id and changes
// nothing. That is "exactly-once effect": at-least-once delivery plus an
// idempotent consumer.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/api/events/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/taskqueue"
)

// KindDeliver is the task kind of a delivery on one channel.
const KindDeliver = "deliver"

// TaskKinds lists every task kind of the service.
var TaskKinds = []string{KindDeliver}

// TxRunner runs fn inside one database transaction carried in ctx.
type TxRunner interface {
	Run(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// ProcessedEvent identifies a consumed event and where it came from.
type ProcessedEvent struct {
	EventID    uuid.UUID
	TransferID uuid.UUID
	Type       string
	Topic      string
	Partition  int32
	Offset     int64
}

// Attempt is one try to deliver a notification on a channel.
type Attempt struct {
	Number   int
	Err      error
	Duration time.Duration
	At       time.Time
	// GiveUp marks the delivery failed for good.
	GiveUp bool
}

// Store stores processed events, notifications and deliveries.
type Store interface {
	// MarkProcessed records the event; first is false when it was recorded
	// before, which makes the event a duplicate.
	MarkProcessed(ctx context.Context, e ProcessedEvent) (first bool, err error)
	// InsertNotification stores n with a pending delivery per channel.
	InsertNotification(ctx context.Context, n notifications.Notification, channels []string) error
	// Notification returns notifications.ErrNotFound for an unknown id.
	Notification(ctx context.Context, id uuid.UUID) (notifications.Notification, error)
	// RecordAttempt stores an attempt and moves the delivery accordingly.
	RecordAttempt(ctx context.Context, notificationID uuid.UUID, channel string, a Attempt) error
	// ByAccount lists an account's notifications, newest first.
	ByAccount(ctx context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error)
}

// Enqueuer schedules deliveries on the task queue.
type Enqueuer interface {
	Enqueue(ctx context.Context, s taskqueue.Spec) (bool, error)
}

// Channel delivers a notification somewhere: a log, a webhook.
type Channel interface {
	Name() string
	Send(ctx context.Context, n notifications.Notification) error
}

// Outcomes of HandleEvent, the outcome label of the consumer metrics.
const (
	OutcomeStored    = "stored"
	OutcomeDuplicate = "duplicate"
	// OutcomeIgnored is an event of a type the contract does not list yet:
	// skipped and committed, not dead-lettered.
	OutcomeIgnored = "ignored"
)

// Metrics receives business measurements, after the commit.
type Metrics interface {
	NotificationCreated(kind notifications.Kind)
	Delivered(channel string, ok bool)
	GaveUp(channel string)
}

// Config holds the tunables of the use cases.
type Config struct {
	// DeliveryMaxAttempts is the number of failed sends after which a
	// delivery is marked failed. It must stay below the task queue's
	// MaxAttempts, or the task goes dead first.
	DeliveryMaxAttempts int
}

// Deps lists the collaborators of Service. Every field is required.
type Deps struct {
	Tx       TxRunner
	Store    Store
	Tasks    Enqueuer
	Channels []Channel
	Clock    Clock
	Metrics  Metrics
	NewID    func() (uuid.UUID, error)
	Logger   *slog.Logger
}

// Service implements the use cases.
type Service struct {
	tx       TxRunner
	store    Store
	tasks    Enqueuer
	channels map[string]Channel
	names    []string
	clock    Clock
	metrics  Metrics
	newID    func() (uuid.UUID, error)
	log      *slog.Logger
	cfg      Config
}

// NewService returns a Service. It panics on a missing dependency or an
// invalid configuration, a wiring bug that must fail at start.
func NewService(d Deps, cfg Config) *Service {
	if cfg.DeliveryMaxAttempts < 1 {
		panic("notifications/app: NewService: DeliveryMaxAttempts must be at least 1")
	}
	if d.Tx == nil || d.Store == nil || d.Tasks == nil || len(d.Channels) == 0 || d.Clock == nil ||
		d.Metrics == nil || d.NewID == nil || d.Logger == nil {
		panic("notifications/app: NewService: missing dependency")
	}
	s := &Service{tx: d.Tx, store: d.Store, tasks: d.Tasks, channels: map[string]Channel{}, clock: d.Clock,
		metrics: d.Metrics, newID: d.NewID, log: d.Logger, cfg: cfg}
	for _, c := range d.Channels {
		if _, dup := s.channels[c.Name()]; dup {
			// Two deliveries of one notification on one channel break the
			// deliveries key: every event would fail.
			panic("notifications/app: NewService: channel " + c.Name() + " given twice")
		}
		s.channels[c.Name()] = c
		s.names = append(s.names, c.Name())
	}
	slices.Sort(s.names)
	return s
}

// Message is one consumed record, as the consumer hands it over.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	// EventID is the event-id header, empty if absent.
	EventID string
}

// HandleEvent turns one consumed message into notifications, exactly once
// per event id. It returns OutcomeStored, OutcomeDuplicate or
// OutcomeIgnored. A message that can never be processed, because it breaks
// the contract or because the store refuses it, fails with
// notifications.ErrInvalidEvent; any other error is transient (the
// database is unreachable), and the consumer retries.
func (s *Service) HandleEvent(ctx context.Context, m Message) (string, error) {
	e, err := s.decode(m)
	if errors.Is(err, eventsv1.ErrUnknownType) {
		// A tolerant reader: the producer may add a type first.
		s.log.DebugContext(ctx, "event of an unknown type skipped", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "err", err)
		return OutcomeIgnored, nil
	}
	if err != nil {
		return "", err
	}
	notes, err := notifications.ForEvent(e, s.newID, s.clock.Now())
	if err != nil {
		return "", err
	}
	outcome := OutcomeDuplicate
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		first, err := s.store.MarkProcessed(ctx, ProcessedEvent{
			EventID: e.ID, TransferID: e.Transfer.ID, Type: string(e.Type),
			Topic: m.Topic, Partition: m.Partition, Offset: m.Offset,
		})
		if err != nil {
			return fmt.Errorf("mark event processed: %w", err)
		}
		if !first {
			return nil
		}
		for _, n := range notes {
			if err := s.store.InsertNotification(ctx, n, s.names); err != nil {
				return fmt.Errorf("store notification: %w", err)
			}
			for _, ch := range s.names {
				payload, _ := json.Marshal(deliverPayload{Channel: ch})
				spec := taskqueue.Spec{Kind: KindDeliver, AggregateID: n.ID, DedupKey: ch, Payload: payload, RunAfter: s.clock.Now()}
				if _, err := s.tasks.Enqueue(ctx, spec); err != nil {
					return fmt.Errorf("enqueue delivery: %w", err)
				}
			}
		}
		outcome = OutcomeStored
		return nil
	})
	if err != nil {
		return "", err
	}
	if outcome == OutcomeStored {
		for _, n := range notes {
			s.metrics.NotificationCreated(n.Kind)
		}
	}
	s.log.DebugContext(ctx, "event handled", "event_id", e.ID, "type", e.Type, "outcome", outcome)
	return outcome, nil
}

// decode checks a message against the contract. Everything wrong here is
// wrong forever, so it is ErrInvalidEvent.
func (s *Service) decode(m Message) (eventsv1.TransferEvent, error) {
	e, err := eventsv1.Decode(m.Value)
	if err != nil {
		return eventsv1.TransferEvent{}, fmt.Errorf("%w: %w", notifications.ErrInvalidEvent, err)
	}
	if m.EventID != "" && m.EventID != e.ID.String() {
		return eventsv1.TransferEvent{}, fmt.Errorf("%w: header event id %q, body %s", notifications.ErrInvalidEvent, m.EventID, e.ID)
	}
	if len(m.Key) > 0 && string(m.Key) != e.Transfer.ID.String() {
		// The key decides the partition and with it the order of a
		// transfer's events; a wrong key is a producer bug.
		return eventsv1.TransferEvent{}, fmt.Errorf("%w: key %q is not the transfer id", notifications.ErrInvalidEvent, m.Key)
	}
	return e, nil
}

type deliverPayload struct {
	Channel string `json:"channel"`
}

// HandleTask runs one delivery; the queue worker calls it.
func (s *Service) HandleTask(ctx context.Context, t taskqueue.Task) error {
	if t.Kind != KindDeliver {
		return taskqueue.Permanent(fmt.Errorf("unknown task kind %q", t.Kind))
	}
	var p deliverPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return taskqueue.Permanent(fmt.Errorf("deliver: bad payload: %w", err))
	}
	return s.deliver(ctx, t.AggregateID, p.Channel, t.Attempts)
}

// deliver sends a notification on one channel. A failed send is retried by
// the queue with backoff; after DeliveryMaxAttempts failures the delivery
// is marked failed and the task ends, so a dead channel does not fill the
// dead-task alert. A channel may receive a notification twice if the
// process dies between the send and the record of it: webhooks carry the
// notification id as Idempotency-Key for the receiver to deduplicate.
func (s *Service) deliver(ctx context.Context, id uuid.UUID, channel string, failures int) error {
	ch, ok := s.channels[channel]
	if !ok {
		return taskqueue.Permanent(fmt.Errorf("deliver: unknown channel %q", channel))
	}
	n, err := s.store.Notification(ctx, id)
	if errors.Is(err, notifications.ErrNotFound) {
		return taskqueue.Permanent(err)
	}
	if err != nil {
		return fmt.Errorf("read notification: %w", err)
	}
	d, ok := n.Delivery(channel)
	if !ok || d.State != notifications.DeliveryPending {
		return nil // already delivered or given up
	}
	if failures >= s.cfg.DeliveryMaxAttempts {
		if err := s.store.RecordAttempt(ctx, id, channel, Attempt{Number: d.Attempts, At: s.clock.Now(), GiveUp: true,
			Err: errors.New("gave up")}); err != nil {
			return fmt.Errorf("record give-up: %w", err)
		}
		s.metrics.GaveUp(channel)
		s.log.ErrorContext(ctx, "delivery failed for good", "notification_id", id, "channel", channel, "attempts", d.Attempts)
		return nil
	}

	start := time.Now()
	sendErr := ch.Send(ctx, n)
	a := Attempt{Number: d.Attempts + 1, Err: sendErr, Duration: time.Since(start), At: s.clock.Now()}
	if err := s.store.RecordAttempt(ctx, id, channel, a); err != nil {
		return errors.Join(sendErr, fmt.Errorf("record attempt: %w", err))
	}
	s.metrics.Delivered(channel, sendErr == nil)
	if sendErr != nil {
		return fmt.Errorf("deliver on %s: %w", channel, sendErr)
	}
	return nil
}

// MaxListLimit bounds a page of the feed.
const MaxListLimit = 200

// Notifications lists an account's notifications, newest first.
func (s *Service) Notifications(ctx context.Context, account uuid.UUID, limit int) ([]notifications.Notification, error) {
	if limit <= 0 || limit > MaxListLimit {
		limit = 50
	}
	return s.store.ByAccount(ctx, account, limit)
}
