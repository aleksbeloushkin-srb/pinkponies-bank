-- +goose Up

-- One row per consumed event, written in the same transaction as the
-- notifications it produced. The primary key is the deduplication: a
-- redelivered event finds its id and changes nothing.
CREATE TABLE processed_events (
    event_id     uuid        PRIMARY KEY,
    transfer_id  uuid        NOT NULL,
    type         text        NOT NULL,
    topic        text        NOT NULL,
    partition    integer     NOT NULL,
    kafka_offset bigint      NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id          uuid        PRIMARY KEY,
    event_id    uuid        NOT NULL REFERENCES processed_events (event_id),
    account_id  uuid        NOT NULL,
    transfer_id uuid        NOT NULL,
    kind        text        NOT NULL,
    message     text        NOT NULL,
    created_at  timestamptz NOT NULL,
    -- An event tells each account at most once, whatever the code does.
    CONSTRAINT notifications_event_account_key UNIQUE (event_id, account_id)
);

-- The feed reads an account's notifications, newest first.
CREATE INDEX notifications_account_idx ON notifications (account_id, created_at DESC, id DESC);

-- One row per notification and channel: where its delivery stands.
CREATE TABLE deliveries (
    notification_id uuid        NOT NULL REFERENCES notifications (id),
    channel         text        NOT NULL,
    state           text        NOT NULL CHECK (state IN ('pending', 'delivered', 'failed')),
    attempts        integer     NOT NULL DEFAULT 0,
    last_error      text        NULL,
    delivered_at    timestamptz NULL,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (notification_id, channel)
);

-- Every try, for the record.
CREATE TABLE delivery_attempts (
    id              bigserial   PRIMARY KEY,
    notification_id uuid        NOT NULL,
    channel         text        NOT NULL,
    attempt         integer     NOT NULL,
    outcome         text        NOT NULL CHECK (outcome IN ('delivered', 'failed', 'gave_up')),
    error           text        NULL,
    duration_ms     integer     NOT NULL,
    at              timestamptz NOT NULL,
    FOREIGN KEY (notification_id, channel) REFERENCES deliveries (notification_id, channel)
);

-- Deliveries run on the shared task queue (internal/platform/taskqueue).
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

-- +goose Down
DROP TABLE tasks;
DROP TABLE delivery_attempts;
DROP TABLE deliveries;
DROP TABLE notifications;
DROP TABLE processed_events;
