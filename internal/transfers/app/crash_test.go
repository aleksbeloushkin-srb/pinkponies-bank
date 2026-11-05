package app

import (
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
)

// sagaPath is one way through the saga and what must hold at its end.
type sagaPath struct {
	name     string
	amount   int64
	status   transfers.Status
	reason   transfers.Reason
	placed   int // holds the ledger executed
	captured int
	released int
	screened int
	events   []transfers.EventType
}

var sagaPaths = []sagaPath{
	{"approve", 1250, transfers.StatusCompleted, "", 1, 1, 0, 1,
		eventsOf(transfers.EventCreated, transfers.EventHeld, transfers.EventCompleted)},
	{"decline", 1266, transfers.StatusRejected, transfers.ReasonScreeningDeclined, 1, 0, 1, 1,
		eventsOf(transfers.EventCreated, transfers.EventHeld, transfers.EventRejected)},
	{"review then approve", 1277, transfers.StatusCompleted, "", 1, 1, 0, 1,
		eventsOf(transfers.EventCreated, transfers.EventHeld, transfers.EventCompleted)},
	{"insufficient funds", 10_001, transfers.StatusRejected, transfers.ReasonInsufficientFunds, 0, 0, 0, 0,
		eventsOf(transfers.EventCreated, transfers.EventRejected)},
}

// remoteCalls numbers the remote calls of a fixture (ledger and screener
// together) and runs at, if set, after the n-th one has been executed.
type remoteCalls struct {
	names []string
	at    int
	do    func(name string) error
}

func (rc *remoteCalls) install(f *fixture) {
	f.ledger.after = func(op string) error { return rc.call(op) }
	f.screener.after = func() error { return rc.call("screen") }
}

func (rc *remoteCalls) call(name string) error {
	rc.names = append(rc.names, name)
	if len(rc.names) == rc.at {
		return rc.do(name)
	}
	return nil
}

// TestEveryCrashPoint runs each saga path once cleanly to count its
// transactions and remote calls, then once per crash point:
//
//   - before the commit of transaction k (rolled back, process dies);
//   - after the commit of transaction k (process dies before going on);
//   - after remote call k was executed, process dies before the answer;
//   - after remote call k was executed, the answer is lost (ambiguous).
//
// After each crash the saga runs to its end, as a restarted process would,
// and the outcome must be the clean one: one hold, exactly one capture or
// one release, the source debited at most once, one event per status.
func TestEveryCrashPoint(t *testing.T) {
	for _, path := range sagaPaths {
		t.Run(path.name, func(t *testing.T) {
			clean := newFixture(t)
			rc := &remoteCalls{}
			rc.install(clean)
			id := clean.create(t, path.amount)
			clean.drain(t)
			checkPath(t, clean, id, path)
			txs, calls := clean.store.runs, slices.Clone(rc.names)

			for k := 1; k <= txs; k++ {
				t.Run(fmt.Sprintf("before commit of tx %d", k), func(t *testing.T) {
					f := newFixture(t)
					f.store.crashBefore = k
					runToEnd(t, f, path)
				})
				t.Run(fmt.Sprintf("after commit of tx %d", k), func(t *testing.T) {
					f := newFixture(t)
					f.store.crashAfter = k
					runToEnd(t, f, path)
				})
			}
			for k, name := range calls {
				t.Run(fmt.Sprintf("crash after call %d (%s)", k+1, name), func(t *testing.T) {
					f := newFixture(t)
					(&remoteCalls{at: k + 1, do: func(string) error { panic(errCrash) }}).install(f)
					runToEnd(t, f, path)
				})
				t.Run(fmt.Sprintf("answer to call %d (%s) lost", k+1, name), func(t *testing.T) {
					f := newFixture(t)
					(&remoteCalls{at: k + 1, do: func(name string) error {
						if name == "screen" {
							return fmt.Errorf("%w: connection reset after the request was written", ErrScreeningUnavailable)
						}
						return fmt.Errorf("%w: DeadlineExceeded", ErrLedgerUnavailable)
					}}).install(f)
					runToEnd(t, f, path)
				})
			}
		})
	}
}

func runToEnd(t *testing.T, f *fixture, path sagaPath) {
	t.Helper()
	id := f.create(t, path.amount)
	f.drain(t)
	checkPath(t, f, id, path)
}

func checkPath(t *testing.T, f *fixture, id uuid.UUID, path sagaPath) {
	t.Helper()
	tr := f.transfer(t, id)
	if tr.Status != path.status || tr.Reason != path.reason {
		t.Fatalf("transfer = %s %q, want %s %q", tr.Status, tr.Reason, path.status, path.reason)
	}
	placed, captured, released := f.ledger.counts()
	if placed != path.placed || captured != path.captured || released != path.released {
		t.Fatalf("ledger executed %d holds, %d captures, %d releases; want %d, %d, %d",
			placed, captured, released, path.placed, path.captured, path.released)
	}
	if captured+released > 1 {
		t.Fatal("a transfer must end with one capture or one release, never both")
	}
	if n, _ := f.screener.stats(); n != path.screened {
		t.Fatalf("screened %d times, want %d", n, path.screened)
	}
	// The source starts with 10 000 and is debited once if the transfer
	// completed, never otherwise.
	wantPosted := int64(10_000) - int64(path.captured)*path.amount
	if src := f.ledger.account(f.src); src.PostedMinor != wantPosted || src.HeldMinor != 0 {
		t.Fatalf("source posted %d held %d, want %d and 0", src.PostedMinor, src.HeldMinor, wantPosted)
	}
	if got := f.store.eventTypes(id); !slices.Equal(got, path.events) {
		t.Fatalf("events = %v, want %v", got, path.events)
	}
	for kind, state := range f.store.taskStates() {
		if state != "done" {
			t.Fatalf("task %s is %s", kind, state)
		}
	}
}
