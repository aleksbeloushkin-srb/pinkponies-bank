package main

import (
	"strings"
	"testing"
)

func baseEnv() []string {
	return []string{
		"NOTIFICATIONS_DATABASE_URL=postgres://localhost/notifications",
		"NOTIFICATIONS_KAFKA_BROKERS=redpanda:9092",
		"NOTIFICATIONS_WEBHOOK_URL=http://localhost:8081/_sink/webhooks",
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(baseEnv())
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.KafkaTopic != "transfers.events" || cfg.KafkaDLQTopic != "transfers.events.dlq" || cfg.KafkaGroup != "notifications" ||
		len(cfg.Channels) != 2 || cfg.SinkEnabled {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{"unknown variable", []string{"NOTIFICATIONS_CHANELS=log"}, "NOTIFICATIONS_CHANELS: unknown variable"},
		{"no brokers", []string{"NOTIFICATIONS_KAFKA_BROKERS="}, "KAFKA_BROKERS is required"},
		{"dlq loops", []string{"NOTIFICATIONS_KAFKA_DLQ_TOPIC=transfers.events"}, "KAFKA_DLQ_TOPIC must differ"},
		{"unknown channel", []string{"NOTIFICATIONS_CHANNELS=log,pigeon"}, `unknown channel "pigeon"`},
		{"webhook without url", []string{"NOTIFICATIONS_WEBHOOK_URL="}, "WEBHOOK_URL must be"},
		{"bad origin", []string{"NOTIFICATIONS_CORS_ORIGINS=localhost:8080"}, "is not an origin"},
		{"origin with path", []string{"NOTIFICATIONS_CORS_ORIGINS=http://localhost:8080/ui"}, "is not an origin"},
		{"faults without sink", []string{"NOTIFICATIONS_SINK_FAULTS=500"}, "SINK_FAULTS needs SINK_ENABLED"},
		{"delivery bound", []string{"NOTIFICATIONS_DELIVERY_MAX_ATTEMPTS=12"}, "TASK_MAX_ATTEMPTS must exceed DELIVERY_MAX_ATTEMPTS"},
		{"webhook budget", []string{"NOTIFICATIONS_WEBHOOK_TIMEOUT=6s"}, "TASK_TIMEOUT must exceed WEBHOOK_TIMEOUT"},
		{"bool", []string{"NOTIFICATIONS_SINK_ENABLED=yes"}, `"yes" is not true or false`},
		{"repeated channel", []string{"NOTIFICATIONS_CHANNELS=log,webhook,log"}, `CHANNELS: "log" is listed twice`},
		{"unknown sink fault", []string{"NOTIFICATIONS_SINK_ENABLED=true", "NOTIFICATIONS_SINK_FAULTS=500,teapot"}, `SINK_FAULTS: unknown sink fault "teapot"`},
		{"lock vs statement", []string{"NOTIFICATIONS_DB_LOCK_TIMEOUT=5s"}, "DB_LOCK_TIMEOUT must be shorter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(append(baseEnv(), tt.env...))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
	if _, err := loadConfig([]string{"NOTIFICATIONS_DATABASE_URL=x", "NOTIFICATIONS_KAFKA_BROKERS=k", "NOTIFICATIONS_CHANNELS=log"}); err != nil {
		t.Fatalf("the log channel alone needs no webhook URL: %v", err)
	}
}
