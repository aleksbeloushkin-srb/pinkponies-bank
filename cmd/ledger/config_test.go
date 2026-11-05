package main

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"LEDGER_DATABASE_URL=postgres://localhost/ledger",
		"HOME=/root", // variables without the prefix are ignored
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(baseEnv())
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.GRPCAddr != ":50051" || cfg.AdminAddr != ":9101" || cfg.HoldDefaultTTL != 15*time.Minute || cfg.SweepBatch != 100 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{"unknown variable", []string{"LEDGER_HOLD_TTL=1m"}, "LEDGER_HOLD_TTL: unknown variable"},
		{"no database", []string{"LEDGER_DATABASE_URL="}, "DATABASE_URL is required"},
		{"bad duration", []string{"LEDGER_SWEEP_INTERVAL=often"}, "LEDGER_SWEEP_INTERVAL"},
		{"zero ttl", []string{"LEDGER_HOLD_DEFAULT_TTL=0s"}, "HOLD_DEFAULT_TTL must be positive"},
		{"default above max", []string{"LEDGER_HOLD_DEFAULT_TTL=2h", "LEDGER_HOLD_MAX_TTL=1h"}, "must not exceed HOLD_MAX_TTL"},
		{"same port", []string{"LEDGER_ADMIN_ADDR=:50051"}, "ADMIN_ADDR must differ"},
		{"lock vs statement", []string{"LEDGER_DB_LOCK_TIMEOUT=5s"}, "DB_LOCK_TIMEOUT must be shorter"},
		{"tiny pool", []string{"LEDGER_DB_MAX_CONNS=1"}, "DB_MAX_CONNS must be between"},
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
