package platform

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Loader reads the variables of one service, all of which share a prefix
// such as "LEDGER_". It remembers which variables it read and collects parse
// errors instead of stopping at the first, so one start-up reports every
// mistake at once.
//
// Any variable with the prefix that no call reads is an error (see Err): a
// typo must not fall back to a default silently.
type Loader struct {
	prefix string
	env    map[string]string
	used   map[string]bool
	errs   []error
}

// NewLoader returns a Loader over environ, given in os.Environ format.
func NewLoader(prefix string, environ []string) *Loader {
	env := make(map[string]string)
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, prefix) {
			env[k] = v
		}
	}
	return &Loader{prefix: prefix, env: env, used: make(map[string]bool)}
}

// Prefix returns the variable prefix, for error messages.
func (l *Loader) Prefix() string { return l.prefix }

func (l *Loader) lookup(name string) (string, bool) {
	key := l.prefix + name
	l.used[key] = true
	v, ok := l.env[key]
	return v, ok
}

func (l *Loader) fail(name string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s%s: %w", l.prefix, name, err))
}

// Fail records a parse error of a variable that a service reads itself,
// such as a list of roles.
func (l *Loader) Fail(name string, err error) { l.fail(name, err) }

// String returns the variable, or def when it is not set.
func (l *Loader) String(name, def string) string {
	if v, ok := l.lookup(name); ok {
		return v
	}
	return def
}

// Duration parses a time.ParseDuration value.
func (l *Loader) Duration(name string, def time.Duration) time.Duration {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(name, err)
	}
	return d
}

// Int parses a decimal integer.
func (l *Loader) Int(name string, def int) int {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(name, err)
	}
	return n
}

// Bool parses "true" or "false". Other spellings that strconv.ParseBool
// accepts ("1", "t") are rejected to keep configuration files uniform.
func (l *Loader) Bool(name string, def bool) bool {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	switch v {
	case "true":
		return true
	case "false":
		return false
	}
	l.fail(name, fmt.Errorf("%q is not true or false", v))
	return def
}

// Level parses a slog level name: debug, info, warn or error.
func (l *Loader) Level(name string, def slog.Level) slog.Level {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(v)); err != nil {
		l.fail(name, err)
	}
	return lv
}

// OneOf reads a value that must be one of allowed.
func (l *Loader) OneOf(name, def string, allowed ...string) string {
	v := l.String(name, def)
	if !slices.Contains(allowed, v) {
		l.fail(name, fmt.Errorf("%q is not one of %v", v, allowed))
	}
	return v
}

// List reads a comma-separated list. Empty items are dropped and the rest
// are trimmed.
func (l *Loader) List(name, def string) []string {
	var out []string
	for _, item := range strings.Split(l.String(name, def), ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Err returns every parse error and every unknown variable, or nil. Call it
// after all reads.
func (l *Loader) Err() error {
	errs := slices.Clone(l.errs)
	for _, k := range slices.Sorted(maps.Keys(l.env)) {
		if !l.used[k] {
			errs = append(errs, fmt.Errorf("%s: unknown variable", k))
		}
	}
	return errors.Join(errs...)
}

// Checks collects validation failures of a loaded configuration. Each
// message is prefixed with the service's variable prefix.
type Checks struct {
	prefix string
	errs   []error
}

// NewChecks returns an empty Checks for variables with prefix.
func NewChecks(prefix string) *Checks {
	return &Checks{prefix: prefix}
}

// Check records a failure when ok is false. format names the variables
// without their prefix; the prefix is added in front.
func (c *Checks) Check(ok bool, format string, args ...any) {
	if !ok {
		c.errs = append(c.errs, fmt.Errorf(c.prefix+format, args...))
	}
}

// Positive checks that every duration is above zero. A zero timeout means
// "already expired" or "no limit", and neither is ever intended.
func (c *Checks) Positive(durations map[string]time.Duration) {
	for _, name := range slices.Sorted(maps.Keys(durations)) {
		c.Check(durations[name] > 0, "%s must be positive", name)
	}
}

// Err returns the collected failures, or nil.
func (c *Checks) Err() error {
	return errors.Join(c.errs...)
}
