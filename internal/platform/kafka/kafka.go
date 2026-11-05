// Package kafka holds the franz-go plumbing shared by the services: client
// construction with a slog logger, topic creation for tests and local
// setups, and a collector that exports consumer lag and topic sizes.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is what every client needs.
type Config struct {
	Brokers  []string
	ClientID string
}

// NewClient returns a franz-go client for cfg with the options every
// service shares, plus the given producer or consumer options. The client
// connects lazily; Ping checks the brokers.
func NewClient(cfg Config, log *slog.Logger, opts ...kgo.Opt) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers")
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithLogger(logger{log: log.With("component", "kafka")}),
		kgo.DialTimeout(5 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	return cl, nil
}

// logger adapts slog to kgo.Logger. franz-go logs every connection at info
// level, so only warnings and errors are passed on.
type logger struct{ log *slog.Logger }

func (l logger) Level() kgo.LogLevel { return kgo.LogLevelWarn }

func (l logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	switch level {
	case kgo.LogLevelError:
		l.log.Error(msg, keyvals...)
	case kgo.LogLevelWarn:
		l.log.Warn(msg, keyvals...)
	case kgo.LogLevelInfo:
		l.log.Info(msg, keyvals...)
	default:
		l.log.Debug(msg, keyvals...)
	}
}

// Topic describes a topic to create.
type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
}

// EnsureTopics creates the topics that do not exist yet and leaves existing
// ones as they are. Production topics belong to infrastructure code; this
// is for tests and the local stack.
func EnsureTopics(ctx context.Context, cl *kgo.Client, topics ...Topic) error {
	adm := kadm.NewClient(cl)
	for _, t := range topics {
		if _, err := adm.CreateTopic(ctx, t.Partitions, t.ReplicationFactor, nil, t.Name); err != nil &&
			!errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: create topic %s: %w", t.Name, err)
		}
	}
	return nil
}
