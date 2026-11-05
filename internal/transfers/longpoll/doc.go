// Package longpoll implements change notification for
// GET /v1/transfers/{id}/wait.
//
// Waiters register in a per-instance Registry with the version they have
// already seen. One Batcher goroutine per instance asks the database for the
// current versions of all awaited transfers in a single query and wakes
// every waiter whose transfer moved past its marker. Database cost is
// therefore one query per tick per instance, not one per waiting client.
//
// Markers make the scheme robust: a waiter is woken only when it observes a
// version strictly greater than the one it has seen, so a duplicate
// wake-up, a late registration or a read from a lagging source can delay an
// answer but never produce a wrong one.
package longpoll

import "errors"

var (
	// ErrTooManyWaiters means the registry of this instance is full. The
	// HTTP layer answers 429 with Retry-After.
	ErrTooManyWaiters = errors.New("too many waiters")
	// ErrClosed means the instance is shutting down. The HTTP layer
	// answers 503.
	ErrClosed = errors.New("long polling is shutting down")
)
