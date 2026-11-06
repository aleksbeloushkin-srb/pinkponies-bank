package main

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"TRANSFERS_DATABASE_URL=postgres://localhost/transfers",
		"TRANSFERS_LEDGER_ADDR=dns:///ledger:50051",
		"TRANSFERS_RISK_URL=http://fakerisk:8090",
		"TRANSFERS_KAFKA_BROKERS=redpanda:9092",
		"HOME=/root", // variables without the prefix are ignored
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(baseEnv())
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if !cfg.Roles.API || !cfg.Roles.Worker || !cfg.Roles.Relay || cfg.KafkaTopic != "transfers.events" {
		t.Fatalf("roles = %+v", cfg.Roles)
	}
	if cfg.HTTPAddr != ":8080" || cfg.AdminAddr != ":9100" || cfg.TaskLease != 30*time.Second || cfg.HoldTTL != 15*time.Minute {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{"unknown variable", []string{"TRANSFERS_HOLD_TLL=1m"}, "TRANSFERS_HOLD_TLL: unknown variable"},
		{"unknown role", []string{"TRANSFERS_ROLES=api,cron"}, `unknown role "cron"`},
		{"no roles", []string{"TRANSFERS_ROLES=,"}, "ROLES must name at least one role"},
		{"no ledger", []string{"TRANSFERS_LEDGER_ADDR="}, "LEDGER_ADDR is required"},
		{"bad risk url", []string{"TRANSFERS_RISK_URL=fakerisk:8090"}, "RISK_URL must be an http(s) URL"},
		{"write timeout", []string{"TRANSFERS_HTTP_WRITE_TIMEOUT=16s"}, "HTTP_WRITE_TIMEOUT must exceed"},
		{"lease", []string{"TRANSFERS_TASK_LEASE=20s"}, "TASK_TIMEOUT must be at least 10s shorter"},
		{"call budget", []string{"TRANSFERS_RISK_TIMEOUT=12s"}, "TASK_TIMEOUT must exceed LEDGER_TIMEOUT and RISK_TIMEOUT"},
		{"screen bound", []string{"TRANSFERS_SCREEN_MAX_FAILURES=20"}, "TASK_MAX_ATTEMPTS must exceed SCREEN_MAX_FAILURES"},
		{"hold ttl", []string{"TRANSFERS_HOLD_TTL=5m"}, "HOLD_TTL must exceed SCREEN_REVIEW_TIMEOUT"},
		{"retry-after cap", []string{"TRANSFERS_RISK_MAX_RETRY_AFTER=5m"}, "RISK_MAX_RETRY_AFTER must be shorter than SCREEN_REVIEW_TIMEOUT"},
		{"pool", []string{"TRANSFERS_DB_MAX_CONNS=6"}, "WORKER_CONCURRENCY must be at most"},
		{"poll bounds", []string{"TRANSFERS_SCREEN_POLL_MIN=1m"}, "SCREEN_POLL_MIN must not exceed"},
		{"admin port", []string{"TRANSFERS_ADMIN_ADDR=:8080"}, "ADMIN_ADDR must differ"},
		{"ui url", []string{"TRANSFERS_UI_NOTIFICATIONS_URL=localhost:8081"}, "UI_NOTIFICATIONS_URL must be"},
		{"no brokers", []string{"TRANSFERS_KAFKA_BROKERS="}, "KAFKA_BROKERS is required"},
		{"relay lease", []string{"TRANSFERS_RELAY_LEASE=8s"}, "RELAY_LEASE must be at least twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(append(baseEnv(), tt.env...))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

// An api-only process needs no screening provider.
func TestAPIRoleNeedsNoRiskURL(t *testing.T) {
	env := []string{"TRANSFERS_DATABASE_URL=postgres://x/y", "TRANSFERS_LEDGER_ADDR=dns:///ledger:50051", "TRANSFERS_ROLES=api"}
	if _, err := loadConfig(env); err != nil { // no RISK_URL, no KAFKA_BROKERS
		t.Fatalf("loadConfig: %v", err)
	}
}
