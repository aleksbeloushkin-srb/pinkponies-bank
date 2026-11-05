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

func TestSummarize(t *testing.T) {
	lags, sizes := summarize([]partitionOffsets{
		{topic: "events", start: 0, end: 10, committed: 7}, // 3 behind
		{topic: "events", start: 0, end: 5, committed: -1}, // never committed: all 5
		{topic: "events", start: 4, end: 6, committed: 2},  // commit before the retained start
		{topic: "dlq", start: 3, end: 3, committed: -1},    // empty
		{topic: "dlq2", start: 1, end: 4, committed: -1},
	})
	if lags["events"] != 3+5+2 || sizes["events"] != 10+5+2 {
		t.Fatalf("events: lag %d size %d", lags["events"], sizes["events"])
	}
	if sizes["dlq"] != 0 || sizes["dlq2"] != 3 {
		t.Fatalf("sizes = %v", sizes)
	}
}
