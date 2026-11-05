-- +goose Up

-- One row per account. posted_minor is the sum of the account's entries and
-- held_minor the sum of its active holds; both are kept up to date in the
-- transaction that changes them, under the account's row lock.
CREATE TABLE accounts (
    id              uuid        PRIMARY KEY,
    name            text        NOT NULL,
    currency        text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    kind            text        NOT NULL CHECK (kind IN ('customer', 'system')),
    posted_minor    bigint      NOT NULL DEFAULT 0,
    held_minor      bigint      NOT NULL DEFAULT 0 CHECK (held_minor >= 0),
    -- The OpenAccount request that created a customer account.
    idempotency_key text        NULL,
    request_hash    text        NULL,
    -- 'treasury:EUR' and the like, for system accounts.
    system_key      text        NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT accounts_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT accounts_system_key_key UNIQUE (system_key),
    CONSTRAINT accounts_keys_by_kind CHECK (
        (kind = 'customer' AND idempotency_key IS NOT NULL AND system_key IS NULL) OR
        (kind = 'system' AND idempotency_key IS NULL AND system_key IS NOT NULL)),
    -- A customer never spends money that is not there, whatever the code
    -- does: the database refuses the commit.
    CONSTRAINT accounts_customer_available_non_negative CHECK (kind = 'system' OR posted_minor - held_minor >= 0)
);

-- A hold reserves money on account_id for a transfer to
-- destination_account_id. Only an active hold counts towards held_minor.
CREATE TABLE holds (
    id                     uuid        PRIMARY KEY,
    account_id             uuid        NOT NULL REFERENCES accounts (id),
    destination_account_id uuid        NOT NULL REFERENCES accounts (id),
    amount_minor           bigint      NOT NULL CHECK (amount_minor > 0),
    currency               text        NOT NULL,
    status                 text        NOT NULL CHECK (status IN ('active', 'captured', 'released')),
    idempotency_key        text        NOT NULL,
    request_hash           text        NOT NULL,
    release_reason         text        NULL,
    release_key            text        NULL,
    transaction_id         uuid        NULL,
    expires_at             timestamptz NOT NULL,
    created_at             timestamptz NOT NULL,
    updated_at             timestamptz NOT NULL,
    CONSTRAINT holds_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT holds_distinct_accounts CHECK (account_id <> destination_account_id),
    CONSTRAINT holds_captured_has_transaction CHECK ((status = 'captured') = (transaction_id IS NOT NULL))
);

-- The expiry sweep and the hold-age gauge read active holds by expiry.
CREATE INDEX holds_active_expires_idx ON holds (expires_at) WHERE status = 'active';
CREATE INDEX holds_active_created_idx ON holds (created_at) WHERE status = 'active';

-- One balanced posting. A hold is captured by at most one transaction.
CREATE TABLE transactions (
    id              uuid        PRIMARY KEY,
    kind            text        NOT NULL CHECK (kind IN ('deposit', 'transfer')),
    idempotency_key text        NOT NULL,
    hold_id         uuid        NULL REFERENCES holds (id),
    currency        text        NOT NULL,
    created_at      timestamptz NOT NULL,
    CONSTRAINT transactions_idempotency_key_key UNIQUE (idempotency_key),
    CONSTRAINT transactions_hold_id_key UNIQUE (hold_id),
    CONSTRAINT transactions_transfer_has_hold CHECK ((kind = 'transfer') = (hold_id IS NOT NULL))
);

ALTER TABLE holds ADD CONSTRAINT holds_transaction_id_fkey
    FOREIGN KEY (transaction_id) REFERENCES transactions (id);

-- The journal: one row per account per transaction. A negative amount takes
-- money from the account, a positive one adds it.
CREATE TABLE entries (
    id                  bigserial   PRIMARY KEY,
    transaction_id      uuid        NOT NULL REFERENCES transactions (id),
    account_id          uuid        NOT NULL REFERENCES accounts (id),
    amount_minor        bigint      NOT NULL CHECK (amount_minor <> 0),
    balance_after_minor bigint      NOT NULL,
    created_at          timestamptz NOT NULL
);

CREATE INDEX entries_account_idx ON entries (account_id, id);
CREATE INDEX entries_transaction_idx ON entries (transaction_id);

-- Double entry, enforced by the database: when the transaction that inserted
-- entries commits, the entries of each ledger transaction must sum to zero.
-- The trigger is deferred so that the entries of one posting can be inserted
-- one by one.
-- +goose StatementBegin
CREATE FUNCTION entries_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT sum(amount_minor) FROM entries WHERE transaction_id = NEW.transaction_id) <> 0 THEN
        RAISE EXCEPTION 'ledger transaction % does not balance', NEW.transaction_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_check_balanced();

-- The journal is append-only: corrections are new transactions.
-- +goose StatementBegin
CREATE FUNCTION entries_reject_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'entries are append-only' USING ERRCODE = 'object_not_in_prerequisite_state';
END
$$;
-- +goose StatementEnd

CREATE TRIGGER entries_append_only
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_reject_change();

-- +goose Down
DROP TABLE entries;
DROP FUNCTION entries_reject_change();
DROP FUNCTION entries_check_balanced();
ALTER TABLE holds DROP CONSTRAINT holds_transaction_id_fkey;
DROP TABLE transactions;
DROP TABLE holds;
DROP TABLE accounts;
