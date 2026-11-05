# Improvements

The next steps, in the order they would pay off, each with its effect,
cost and risk, and the weakness in [REVIEW.md](REVIEW.md) it addresses.

| # | Change | Fixes | Effect | Cost | Risk |
| --- | --- | --- | --- | --- | --- |
| 1 | Operator tooling: requeue dead tasks, inspect a saga, replay the dead-letter topic | W6, W7 | a dead capture is settled in a minute instead of with SQL | low | low |
| 2 | Relay order per transfer with several relays | W2 | the relay can scale past one instance without reordering a transfer's events | low to medium | low |
| 3 | Relay throughput: produce a batch, mark the per-partition prefix that was acknowledged | W3 | hundreds of events per round trip instead of one | medium | medium |
| 4 | Consumer: one goroutine per assigned partition | W1 | a failing partition no longer stalls the others; rebalances no longer wait for retries | medium | medium |
| 5 | Retention: delete done tasks, published outbox rows, old processed events and attempts | W10 | bounded tables and indexes | low | low |
| 6 | Authentication and authorization; mTLS between services | W11 | a client moves only its own money; the ledger talks only to transfers | medium to high | medium |
| 7 | Tracing (OpenTelemetry) across HTTP, gRPC, the task queue and Kafka headers | W12 | one trace per transfer instead of a grep for its id | medium | low |
| 8 | Contract checks in CI: `buf breaking` against main, a JSON Schema of the event and a consumer-driven test | W13 | a breaking change to the ledger API or the event fails the build | low | low |
| 9 | Screening: a circuit breaker and a bulkhead per provider; park instead of cancel on misconfiguration | W8, W9 | a broken provider cannot take every worker slot or cancel every transfer | low to medium | low |
| 10 | Cross-service configuration check: the ledger publishes its TTL bounds, transfers checks them at start | W5 | a TTL mismatch fails the deploy, not every transfer | low | low |
| 11 | Ledger hot spots: shard the treasury per currency, keep running totals for the balance gauge | W14, W15 | deposits and scrapes stop scaling with accounts | medium | medium |
| 12 | Load tests with failure injection in CI | none | numbers for throughput and the cost of each retry policy | medium | low |

## 1. Operator tooling

Every safety net in this repository ends with "an operator": a dead task,
a message in the dead-letter topic, a hold the sweep released under a
stuck transfer. Today that means SQL against the transfers database or
`rpk` against the topic.

The minimum is an admin endpoint per service, on the admin listener and
behind authentication:

- `POST /admin/tasks/{id}/requeue` sets a dead task back to `ready` with
  zero attempts; the step's idempotency key makes a rerun safe;
- `GET /admin/transfers/{id}` shows the saga log with the tasks and their
  `last_error`;
- `POST /admin/dlq/replay?offset=...` republishes a dead letter to the
  main topic once the consumer is fixed, and records that it did.

**Effect:** the `DeadTasks` and `DLQNotEmpty` runbooks become one call.
**Cost:** low. **Risk:** low, as long as requeueing is audited.

## 2. Relay order with several relays

Two relays claim the oldest unpublished events with `SKIP LOCKED`; if one
claims event 1 of a transfer and the other event 2, event 2 can reach
Kafka first. Two fixes, either enough:

- claim only events whose transfer has no older unpublished event (a
  `NOT EXISTS` on the same aggregate in the claim query, backed by an
  index on `(aggregate_id, id) WHERE published_at IS NULL`);
- give each relay a shard of transfers (hash of the id modulo the number
  of relays), with a lease on the shard.

**Effect:** the relay scales out. **Cost:** a query change and its tests.
**Risk:** low.

## 3. Relay throughput

The relay produces one record and waits for the acknowledgement before the
next: about one broker round trip per event. Produce the whole batch at
once, wait for every promise, and mark published the longest prefix of
acknowledged events per transfer; release the rest. franz-go keeps order
within a partition and fails every buffered record of a partition after a
failure, which keeps a later event of a transfer from landing alone.

**Effect:** throughput bounded by the broker, not by latency. **Cost:**
medium, mostly tests of partial failures. **Risk:** medium: getting the
prefix wrong publishes out of order.

## 4. Partition-parallel consumer

The consumer handles the records of one poll partition after partition.
A transient failure on one partition (the database rejects one insert)
stops the others too, and because rebalances wait while a poll is handled,
a long outage also blocks a rebalance until the member is kicked out.

The common structure is a goroutine per assigned partition, fed by the
poll loop, with `OnPartitionsAssigned` and `OnPartitionsRevoked` starting
and stopping them and committing what they handled.

**Effect:** isolation between partitions. **Cost:** medium. **Risk:**
medium: revocation must wait for the partition's goroutine and commit
before the partition moves.

## 5. Retention

`tasks` rows stay `done`, `outbox` rows stay published, `processed_events`
and `delivery_attempts` grow forever. A daily job deleting in batches
(`DELETE ... WHERE id IN (SELECT ... LIMIT 1000)`) keeps them bounded;
`processed_events` must be kept longer than the longest possible
redelivery (the topic's retention), or a replayed event would no longer be
recognised. At volume, partition the tables by month and drop partitions.

## 6. Authentication, authorization, mTLS

Nothing is authenticated: any client can move money between any two
accounts, and the ledger trusts anyone who can reach its port. A real bank
needs an identity on every request (OAuth 2 tokens at the edge), a check
that the caller owns the source account, and mutual TLS between services
with the ledger accepting only the transfers service.

## 7. Tracing

The transfer id is the request id of every saga step and ledger RPC, which
already makes one grep follow a transfer. A trace would show the timing:
OpenTelemetry spans for HTTP and gRPC (interceptors exist for both), a span
per task run linked to the span that enqueued it, and the trace context in
the Kafka headers so the notification is part of the same trace.

## 8. Contract checks

`make proto-check` proves the generated code is fresh; it does not prove
the change is compatible. `buf breaking --against .git#branch=main` in CI
catches removed or renumbered fields. For events, a JSON Schema generated
from `eventsv1` and a test in the notifications service that decodes the
golden events of the producer would catch a field renamed on one side.

## 9. Screening resilience

A provider that hangs holds a worker slot for `RISK_TIMEOUT` per call;
continuous claiming keeps other steps moving, but a fully hanging provider
still costs every slot that runs a screening. A circuit breaker opens
after a run of failures and fails screenings at once (they are retried
later with the same key, so nothing is lost), and a bulkhead caps the
slots screening may use. Separately, a misconfiguration (401, 404) should
park the step until the configuration is fixed instead of canceling the
transfer after six tries.

## 10. Cross-service configuration

`TRANSFERS_HOLD_TTL` must not exceed `LEDGER_HOLD_MAX_TTL`; no single
configuration can check it. The ledger could expose its bounds in a
`GetLimits` RPC, and the transfers service check them at start and fail
fast, or the transfers service could ask for no TTL and let the ledger's
default apply.

## 11. Ledger hot spots

Every funded account opening locks the treasury row of its currency, and
the balance gauge sums every account on each scrape. Several treasury
accounts per currency, chosen by hash, remove the first; a running total
per currency and kind, updated in the posting transaction, removes the
second.

## 12. Load tests

There are no throughput numbers in this repository. A load test against
the compose stack with fault injection (provider latency, hangs, broker
restarts) would show the cost of each retry policy and where the first
bottleneck is: most likely the single relay (item 3) or the hot source
account of a popular payer.
