-- +goose Up

-- One row per transfer. version is the change marker of long polls and
-- events; it grows by one with every committed change of the transfer or
-- of its saga log.
CREATE TABLE transfers (
    id                     uuid        PRIMARY KEY,
    source_account_id      uuid        NOT NULL,
    destination_account_id uuid        NOT NULL,
    amount_minor           bigint      NOT NULL CHECK (amount_minor > 0),
    currency               text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status                 text        NOT NULL CHECK (status IN ('pending', 'held', 'completed', 'rejected', 'canceled')),
    reason                 text        NULL,
    version                bigint      NOT NULL CHECK (version > 0),
    hold_id                uuid        NULL,
    ledger_transaction_id  uuid        NULL,
    screening_id           text        NULL,
    idempotency_key        text        NOT NULL,
    request_hash           text        NOT NULL,
    created_at             timestamptz NOT NULL,
    updated_at             timestamptz NOT NULL,
    CONSTRAINT transfers_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT transfers_distinct_accounts CHECK (source_account_id <> destination_account_id),
    CONSTRAINT transfers_held_has_hold CHECK (status IN ('pending', 'rejected') OR hold_id IS NOT NULL),
    CONSTRAINT transfers_completed_has_transaction CHECK ((status = 'completed') = (ledger_transaction_id IS NOT NULL))
);

-- The saga gauges read transfers that are not final.
CREATE INDEX transfers_in_flight_idx ON transfers (created_at) WHERE status IN ('pending', 'held');

-- The saga log: one row per step of a transfer. A step left 'running' after
-- a crash says that its remote call may have happened.
CREATE TABLE saga_steps (
    transfer_id     uuid        NOT NULL REFERENCES transfers (id),
    step            text        NOT NULL CHECK (step IN ('hold', 'screen', 'capture', 'release')),
    state           text        NOT NULL CHECK (state IN ('running', 'waiting', 'succeeded', 'failed')),
    outcome         text        NULL,
    attempts        integer     NOT NULL CHECK (attempts > 0),
    -- The key of the step's remote call, the same on every attempt.
    idempotency_key text        NOT NULL,
    position        integer     NOT NULL,
    started_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (transfer_id, step),
    CONSTRAINT saga_steps_key_key UNIQUE (idempotency_key)
);

-- Background commands of the saga (internal/platform/taskqueue). A task is
-- claimed by setting lease_until; it becomes claimable again when the lease
-- expires. (kind, aggregate_id, dedup_key) makes enqueueing idempotent.
CREATE TABLE tasks (
    id           bigserial   PRIMARY KEY,
    kind         text        NOT NULL,
    aggregate_id uuid        NOT NULL,
    dedup_key    text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    attempts     integer     NOT NULL DEFAULT 0,
    run_after    timestamptz NOT NULL,
    lease_until  timestamptz NULL,
    last_error   text        NULL,
    state        text        NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'done', 'dead')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tasks_dedup_key UNIQUE (kind, aggregate_id, dedup_key)
);

CREATE INDEX tasks_ready_idx ON tasks (run_after, id) WHERE state = 'ready';
CREATE INDEX tasks_state_kind_idx ON tasks (state, kind) WHERE state IN ('ready', 'dead');

-- Integration events, written in the same transaction as the change they
-- describe. The relay claims them with a lease, publishes them to Kafka and
-- sets published_at.
CREATE TABLE outbox (
    id            bigserial   PRIMARY KEY,
    event_id      uuid        NOT NULL,
    aggregate_id  uuid        NOT NULL,
    type          text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL,
    published_at  timestamptz NULL,
    claimed_until timestamptz NULL,
    CONSTRAINT outbox_event_id_key UNIQUE (event_id)
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE tasks;
DROP TABLE saga_steps;
DROP TABLE transfers;
