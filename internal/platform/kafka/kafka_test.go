package kafka

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoggerPassesWarningsAndErrors(t *testing.T) {
	var buf bytes.Buffer
	l := logger{log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	if l.Level() != kgo.LogLevelWarn {
		t.Fatalf("level = %v, want warn: franz-go logs every connection at info", l.Level())
	}
	l.Log(kgo.LogLevelError, "broker gone", "broker", 1)
	l.Log(kgo.LogLevelWarn, "retrying", "attempt", 2)
	out := buf.String()
	if !strings.Contains(out, "level=ERROR msg=\"broker gone\" broker=1") || !strings.Contains(out, "level=WARN msg=retrying attempt=2") {
		t.Fatalf("log:\n%s", out)
	}
}

func TestNewClientNeedsBrokers(t *testing.T) {
	if _, err := NewClient(Config{ClientID: "x"}, slog.Default()); err == nil {
		t.Fatal("a client without brokers must be refused")
	}
}
