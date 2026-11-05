# Extending the bank

How to add the things a real bank grows into, and what to check each time.
Each section names the files to touch.

## A new saga step

Say transfers above a limit need a second check, an extra approval, after
screening.

1. **Name it.** Add the step and its outcomes to
   [`internal/transfers/step.go`](../internal/transfers/step.go)
   (`Steps`, `StepOutcomes`) and, if it can refuse, a reason to
   [`internal/transfers/status.go`](../internal/transfers/status.go).
   The outcomes become metric labels; keep them a short fixed list.
2. **Add the task kind** to `TaskKinds` in
   [`internal/transfers/app/service.go`](../internal/transfers/app/service.go)
   and a case to `HandleTask`.
3. **Write the handler** in `saga.go` with the same three parts as the
   others: `beginStep` (checks the status and that the step is still
   open), the call with `transfers.StepKey(id, step)` as its idempotency
   key, and `finishStep` (applies the answer and enqueues the next step).
   Decide which errors are retried, which are bounded and which are
   permanent, and write that down in [SAGA.md](SAGA.md).
4. **Decide where it sits relative to the pivot.** Before capture, every
   refusal must end in `compensate(...)`, which releases the hold. After
   capture there is no way back short of a new transfer in the other
   direction.
5. **Change the previous step** to enqueue the new one instead of capture.
6. **Add it to the crash-point tests:** the path table in
   [`internal/transfers/app/crash_test.go`](../internal/transfers/app/crash_test.go)
   picks up every new transaction and remote call by itself; add a path
   that goes through the new refusal.

The schema needs one change: `saga_steps.step` has a check constraint
that lists the steps; extend it in a new migration.

## A real screening provider

The saga talks to "the provider" through one port, `app.Screener`
([`internal/transfers/app/ports.go`](../internal/transfers/app/ports.go)).
A real provider is a second adapter next to
[`internal/transfers/riskclient`](../internal/transfers/riskclient/client.go).

1. **Check that it deduplicates by a key you choose**, and for how long.
   The saga's retries are only safe if a repeat returns the earlier
   decision; a provider that keeps keys for an hour needs
   `SCREEN_REVIEW_TIMEOUT` and the task's retry window to stay below that.
2. **Map its decisions** to approve, decline and review. Anything else is
   an error, never a default.
3. **Classify every failure** as in the table in SAGA.md: refused before
   executing (429, a 503 the provider documents as "not processed"),
   maybe executed (timeouts, resets, 5xx, unreadable answers),
   misconfigured (our credentials or request). All of them wrap
   `ErrScreeningUnavailable`; the class is for the metrics.
4. **Respect `Retry-After`:** return an `*app.RetryAfterError`, and the task
   queue waits at least that long.
5. **Keep the per-call deadline** shorter than the task timeout; the
   configuration checks `RISK_TIMEOUT + 5s <= TASK_TIMEOUT`.

## Another consumer of transfer events

Statements, analytics or fraud scoring can read `transfers.events`
without any change to the transfers service.

1. **Depend on the contract only:**
   [`api/events/v1`](../api/events/v1/events.go). Decode with
   `eventsv1.Decode`, which ignores unknown fields.
2. **Use your own consumer group**, so your offsets are yours; a new group
   starts from the beginning of the topic.
3. **Deduplicate by event id** in the same transaction as your effect, as
   [`internal/notifications/app`](../internal/notifications/app/service.go)
   does with `processed_events`. Delivery is at least once.
4. **Order by key, compare versions.** Events of one transfer share a key
   and a partition, so they arrive in order from one relay; their
   `transfer.version` grows, with gaps. If you keep state per transfer,
   ignore an event whose version is not above the one you stored.
5. **Have a dead-letter topic** and an alert on it; reuse
   [`internal/notifications/consumer`](../internal/notifications/consumer/consumer.go)
   if your handler fits its interface.

## A new notification channel

Push, e-mail or SMS: one type with `Name()` and `Send(ctx, notification)`
in [`internal/notifications/channels`](../internal/notifications/channels/channels.go),
one case in `newChannels` in
[`cmd/notifications/main.go`](../cmd/notifications/main.go) and one name in
`knownChannels` in its `config.go`. Every notification gets a delivery per
channel and a task per delivery; failures are retried with backoff and
marked failed after `DELIVERY_MAX_ATTEMPTS`. Send the notification id as
the provider's idempotency key if it has one: a crash between the send and
the record of it sends again.

## A new ledger operation

A refund, a fee, an interest posting: anything that moves money is a
balanced transaction built with `ledger.Post`
([`internal/ledger/journal.go`](../internal/ledger/journal.go)), so the
domain and the database's deferred trigger both check it.

1. Add the RPC and its messages to
   [`api/proto/ledger/v1/ledger.proto`](../api/proto/ledger/v1/ledger.proto)
   with an `idempotency_key`, and run `make proto proto-lint`. Never
   reuse or renumber a field.
2. Add the use case to `internal/ledger/app`: lock rows in the documented
   order (a hold before accounts, accounts by id), look the key up after
   the lock, refuse with a domain error.
3. Add the error to the table in
   [`internal/ledger/grpcapi/errors.go`](../internal/ledger/grpcapi/errors.go)
   and, if clients must tell it apart, a value to `ErrorReason`.
4. Extend the random-operation invariant test
   ([`internal/ledger/app/invariant_test.go`](../internal/ledger/app/invariant_test.go))
   and the concurrency tests in `test/integration/ledger_test.go`.

## Scaling

| Service | Scales by | Watch out for |
| --- | --- | --- |
| transfers API | more instances behind a load balancer | each instance has its own long-poll registry and batcher; the cap is per instance |
| transfers worker | more instances with `TRANSFERS_ROLES=worker` | `SKIP LOCKED` and leases make it safe; each slot holds a connection only during its two short transactions |
| transfers relay | one instance | two relays can publish two events of one transfer in the wrong order (REVIEW.md); shard by transfer if one relay is too slow |
| ledger | more instances | hot accounts serialize on their row lock; deposits serialize on the treasury row of their currency |
| notifications | up to one instance per partition of `transfers.events` (three locally) | more partitions need a topic change; order is per partition |

Roles are selected by environment variable (`TRANSFERS_ROLES=api,worker,relay`),
so the same image runs as API, worker or relay deployments.

## Versioning

- **gRPC:** add fields and methods; for a breaking change, a `ledger.v2`
  package served next to `v1` until every client moved.
- **Events:** add fields freely. A breaking change is `schema_version: 2`,
  published to a new topic or alongside v1 until every consumer moved;
  consumers reject versions they do not know (they go to the DLQ, which
  alerts).
- **REST:** additive changes in `/v1`; a new prefix for anything else.
- **Database:** goose migrations run at start under an advisory lock;
  write them so the previous version of the binary keeps working during a
  rolling deploy (add a column, backfill, use it, then drop the old one).
