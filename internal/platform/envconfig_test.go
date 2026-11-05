package platform

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoaderReadsTypedValues(t *testing.T) {
	l := NewLoader("SVC_", []string{
		"SVC_NAME=pony", "SVC_WAIT=3s", "SVC_COUNT=7", "SVC_ON=true", "SVC_LEVEL=debug",
		"SVC_MODE=b", "SVC_ITEMS= a, ,b ", "OTHER_IGNORED=1",
	})
	if got := l.String("NAME", "x"); got != "pony" {
		t.Errorf("String = %q", got)
	}
	if got := l.String("MISSING", "def"); got != "def" {
		t.Errorf("String default = %q", got)
	}
	if got := l.Duration("WAIT", time.Second); got != 3*time.Second {
		t.Errorf("Duration = %s", got)
	}
	if got := l.Int("COUNT", 0); got != 7 {
		t.Errorf("Int = %d", got)
	}
	if got := l.Bool("ON", false); !got {
		t.Error("Bool = false")
	}
	if got := l.Level("LEVEL", slog.LevelInfo); got != slog.LevelDebug {
		t.Errorf("Level = %v", got)
	}
	if got := l.OneOf("MODE", "a", "a", "b"); got != "b" {
		t.Errorf("OneOf = %q", got)
	}
	if got := l.List("ITEMS", ""); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("List = %q", got)
	}
	if err := l.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
}

// Every mistake is reported in one error, so one restart fixes them all.
func TestLoaderCollectsEveryError(t *testing.T) {
	l := NewLoader("SVC_", []string{
		"SVC_WAIT=soon", "SVC_COUNT=many", "SVC_ON=yes", "SVC_MODE=c", "SVC_TYPO=1",
	})
	l.Duration("WAIT", time.Second)
	l.Int("COUNT", 1)
	l.Bool("ON", false)
	l.OneOf("MODE", "a", "a", "b")

	err := l.Err()
	if err == nil {
		t.Fatal("Err = nil")
	}
	for _, want := range []string{"SVC_WAIT", "SVC_COUNT", `SVC_ON: "yes" is not true or false`, `"c" is not one of [a b]`, "SVC_TYPO: unknown variable"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestChecks(t *testing.T) {
	c := NewChecks("SVC_")
	c.Check(true, "never reported")
	c.Check(false, "%s must exceed %s", "A", "B")
	c.Positive(map[string]time.Duration{"OK": time.Second, "ZERO": 0})

	err := c.Err()
	if err == nil {
		t.Fatal("Err = nil")
	}
	if !strings.Contains(err.Error(), "SVC_A must exceed B") || !strings.Contains(err.Error(), "SVC_ZERO must be positive") {
		t.Fatalf("Err = %v", err)
	}
	if strings.Contains(err.Error(), "OK") || strings.Contains(err.Error(), "never") {
		t.Fatalf("Err reports a passing check: %v", err)
	}
}
