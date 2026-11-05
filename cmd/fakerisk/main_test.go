package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig([]string{"FAKERISK_FAULTS=429,hang", "FAKERISK_REVIEW_DELAY=1s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8090" || len(cfg.Risk.Faults) != 2 || cfg.Risk.ReviewDelay != time.Second {
		t.Fatalf("cfg = %+v", cfg)
	}
	for env, want := range map[string]string{
		"FAKERISK_FAULTS=boom":      `unknown fault "boom"`,
		"FAKERISK_LATENCY=-1s":      "must not be negative",
		"FAKERISK_LATECNY=1s":       "FAKERISK_LATECNY: unknown variable",
		"FAKERISK_RETRY_AFTER=soon": "FAKERISK_RETRY_AFTER",
	} {
		if _, err := loadConfig([]string{env}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", env, err, want)
		}
	}
}
