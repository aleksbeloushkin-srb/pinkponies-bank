# Observability

Every service exports Prometheus metrics on its own admin listener, and
`deploy/prometheus/` holds the scrape configuration and the alert rules
that the local stack runs. Logs are JSON lines with a request id. There is
no tracing yet (IMPROVEMENTS.md).

```sh
curl localhost:9100/metrics       # transfers
curl localhost:9101/metrics       # ledger
curl localhost:9102/metrics       # notifications
open http://localhost:9091/alerts # Prometheus in docker compose
make check-alerts                 # promtool: config check and alert rule tests
```

## How it is wired

- **One registry per process, injected.** Each composition root creates a
  `*prometheus.Registry` and passes it to the constructors that record
  metrics; nothing registers on the global registry, so tests create as
  many independent instances as they like.
- **Each adapter owns its metrics.** HTTP, gRPC (server and client), the
  task queue, the relay, the consumer, the screening client and long
  polling define their metrics next to the code that records them.
  Business metrics go through a `Metrics` port of each `app` package, so
  the use cases stay free of Prometheus.
- **Business metrics count committed work only.** A transfer created, a
  status changed, a hold placed, a notification created: each is reported
  after the transaction commits. Replays and rolled-back attempts never
  move a counter.
- **State is read on scrape.** Collectors run one bounded query per scrape
  against the database (tasks, outbox, sagas in flight, holds, balances)
  or the brokers (consumer lag, topic sizes). On failure they count an
  error and emit no gauges: a frozen "0 dead tasks" would look healthy
  while the database is down, whereas a missing series and the error
  counter both show up.
- **Known label values start at zero.** Counters with an outcome or a kind
  label are created at zero for every known value, so `increase()` sees
  the first event instead of starting from it.
- **Low cardinality.** Labels come from small fixed sets: route patterns,
  gRPC full method names, task kinds, step names and outcomes, channel
  names, topic names. Ids (transfers, accounts, holds, events) never
  become labels.
- **Admin listener.** `/metrics`, `/healthz` and `/readyz` are on a
  separate port that stays up while the API drains, and stops last.

## Metrics

### transfers (`:9100`)

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `http_requests_total` | counter | `route`, `method`, `code` | API requests by route pattern (`/v1/transfers/{id}`), `unmatched` if none matched |
| `http_request_duration_seconds` | histogram | `route`, `method` | API latency, long polls excluded |
| `http_long_poll_duration_seconds` | histogram | none | duration of `GET /v1/transfers/{id}/wait` |
| `transfers_created_total` | counter | none | transfers accepted; replays of an `Idempotency-Key` are not counted |
| `transfer_transitions_total` | counter | `from`, `to` | committed status changes |
| `transfer_time_to_final_seconds` | histogram | `status` | acceptance to `completed`, `rejected` or `canceled` |
| `saga_steps_finished_total` | counter | `step`, `outcome` | committed step outcomes: `hold:placed`, `hold:insufficient_funds`, `screen:approve`, `screen:review`, `capture:captured`, `release:released`... |
| `transfers_in_flight` | gauge | `status` | transfers in `pending` or `held` |
| `transfer_oldest_in_flight_seconds` | gauge | none | age of the oldest transfer that is not final; 0 when none |
| `screening_requests_total` | counter | `outcome` | screening calls by class: `approve`, `decline`, `review`, `rate_limited`, `unavailable`, `ambiguous`, `misconfigured` |
| `screening_request_duration_seconds` | histogram | none | duration of one screening call |
| `grpc_client_handled_total` | counter | `method`, `code` | RPCs to the ledger as this side saw them, including deadlines that expired on the way |
| `grpc_client_handling_seconds` | histogram | `method` | duration of RPCs to the ledger |
| `tasks_claimed_total`, `task_attempts_total` | counter | `kind` (+ `outcome`) | saga steps claimed; settled runs by outcome `ok`, `retry`, `permanent`, `dead` |
| `task_lag_seconds`, `task_duration_seconds` | histogram | `kind` | due-to-claim delay; handler run time including the remote call |
| `lease_lost_total`, `task_panics_total` | counter | none | results discarded after a lease takeover; recovered handler panics |
| `tasks_ready`, `tasks_dead` | gauge | `kind` | tasks in the ready and dead states |
| `task_oldest_ready_seconds` | gauge | none | age of the oldest due task |
| `outbox_published_total`, `outbox_failed_total` | counter | none | events published and marked; failed deliveries (the event is released and retried) |
| `outbox_publish_duration_seconds` | histogram | none | produce to Kafka until acknowledged |
| `outbox_oldest_unpublished_seconds` | gauge | none | age of the oldest unpublished event |
| `longpoll_waiters`, `longpoll_rejected_total`, `longpoll_batch_query_duration_seconds` | gauge, counter, histogram | none | open long polls, polls refused at the cap, the batcher's query |
| `db_pool_*` | gauge, counter | none | connection pool: in use, idle, waits |
| `metrics_collect_errors_total`, `task_collect_errors_total` | counter | none | scrapes in which the database gauges could not be read |

### ledger (`:9101`)

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `grpc_server_handled_total` | counter | `method`, `code` | RPCs by full method name and status code; `AlreadyExists` counts replays |
| `grpc_server_handling_seconds` | histogram | `method` | RPC duration |
| `grpc_server_panics_total` | counter | none | handler panics answered with `Internal` |
| `ledger_holds_placed_total`, `ledger_holds_captured_total` | counter | none | holds placed and captured, replays excluded |
| `ledger_holds_released_total` | counter | `cause` | `requested` (compensation) or `expired` (the sweep) |
| `ledger_holds_active` | gauge | none | holds in the active state |
| `ledger_hold_oldest_active_seconds` | gauge | none | age of the oldest active hold |
| `ledger_balance_sum_minor` | gauge | `currency`, `kind` | sum of posted balances; per currency, customer plus system must be 0 |
| `db_pool_*`, `metrics_collect_errors_total` | | | as above |

### notifications (`:9102`)

| Name | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `consumer_records_total` | counter | `outcome` | records handled: `stored`, `duplicate`, `dead_lettered` |
| `consumer_retries_total` | counter | `class` | failed tries to be retried: `poison` (bounded, then the DLQ) or `transient` (until it works) |
| `consumer_commit_errors_total` | counter | none | failed offset commits; the records come back and are deduplicated |
| `consumer_handle_duration_seconds` | histogram | none | one successful handling |
| `kafka_consumer_group_lag` | gauge | `group`, `topic` | records not committed by the group, from committed and end offsets (see below) |
| `kafka_topic_messages` | gauge | `topic` | records retained in the dead-letter topic |
| `kafka_collect_errors_total` | counter | none | scrapes in which the broker gauges could not be read |
| `notifications_created_total` | counter | `kind` | notifications stored; a duplicate event creates none |
| `deliveries_total` | counter | `channel`, `outcome` | `delivered`, `failed` (will retry), `gave_up` |
| task queue, HTTP and pool metrics | | | as for transfers, for the delivery tasks and the feed |

**Why the lag is computed by hand.** `kadm.Client.Lag` computes the lag of
a group with no members only over partitions that have a commit. A group
with no members is exactly a consumer that is down, and a partition that
never received a commit would read as zero lag. The collector reads the
committed offsets and the start and end offsets itself, and counts a
partition without a commit from its start
([`internal/platform/kafka/collector.go`](../internal/platform/kafka/collector.go)).

## Alerts

Gauges read from shared state are exported identically by every instance
of a service, so the rules aggregate them with `max()`, never `sum()`; a
promtool test runs every rule with two instances where it matters.

| Alert | Expression | For | Severity | First thing to check |
| --- | --- | --- | --- | --- |
| `SagaStuck` | `max(transfer_oldest_in_flight_seconds) > 600` | 5m | page | the oldest pending or held transfer and its saga step left `running` or `waiting`; `tasks_dead`; the ledger and the provider |
| `DeadTasks` | `max by (job, kind) (tasks_dead) > 0` | 1m | ticket | `last_error` of the dead tasks; fix, then set them back to ready (the step's key makes that safe) |
| `OutboxLag` | `max(outbox_oldest_unpublished_seconds) > 60` | 5m | page | the relay and `outbox_failed_total`; Kafka reachability. Events are kept |
| `ScreeningAmbiguousRatio` | `sum(rate(screening_requests_total{outcome="ambiguous"}[10m])) / sum(rate(screening_requests_total[10m])) > 0.05` | 10m | page | the provider's latency and errors; past the bound transfers are canceled |
| `ScreeningMisconfigured` | `sum(increase(screening_requests_total{outcome="misconfigured"}[5m])) > 0` | | page | `TRANSFERS_RISK_URL` and credentials |
| `HoldLeak` | `max(ledger_hold_oldest_active_seconds) > 900` | 5m | page | the transfer behind the hold (its key is `transfer:<id>:hold`); the sweep releases it at its TTL |
| `LedgerOutOfBalance` | `sum by (currency) (max by (currency, kind) (ledger_balance_sum_minor)) != 0` | 1m | page | stop writes; compare `posted_minor` with the sum of entries per account |
| `DLQNotEmpty` | `max(kafka_topic_messages{topic="transfers.events.dlq"}) > 0` | 1m | ticket | the dead letters and their `dlq-error` headers; fix, replay what is valid |
| `ConsumerLag` | `max by (group, topic) (kafka_consumer_group_lag) > 100` | 5m | page | the notifications service and `consumer_retries_total{class="transient"}` |
| `TargetDown` | `up == 0` | 1m | page | the container; its gauges are missing, so other alerts are blind |
| `GaugesUnreadable` | `sum by (job) (increase({__name__=~"metrics_collect_errors_total\|task_collect_errors_total\|kafka_collect_errors_total"}[5m])) > 0` | 5m | ticket | the database or brokers as seen from the service |

`GaugesUnreadable` uses one selector over three counters because each
service exports a different subset of them: adding three vectors would
match no series for a service that lacks one, and the alert would never
fire. The promtool test feeds it a single counter on purpose.

The thresholds are starting points for this reference service: ten
minutes for a saga allows a full review (five minutes) and retries; a hold
older than fifteen minutes is older than any transfer that is not stuck.

## Logs and correlation

Every log line is JSON from `log/slog` with `request_id` when one is known:

- the HTTP middleware keeps a well-formed incoming `X-Request-ID` or makes
  one, and returns it;
- saga steps use the **transfer id** as the request id, and the ledger
  client sends it in the `x-request-id` gRPC metadata, which the ledger's
  interceptor puts in its own logs. One search for a transfer id finds its
  HTTP request, every saga step and every ledger RPC;
- domain values log through `LogValue` allowlists: ids, statuses and
  amounts, never idempotency keys or request hashes.

```sh
docker compose logs transfers ledger | grep 01a1ad5a-8573-77b7-b6c3-3f2f3a7a878e
```
