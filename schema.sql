CREATE TABLE IF NOT EXISTS orders (
    id           uuid        PRIMARY KEY,
    customer_id  text        NOT NULL,
    amount_cents bigint      NOT NULL CHECK (amount_cents > 0),
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- One row = one event decided but not yet published. Written in the same
-- transaction as the orders row; that is the whole pattern.
CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial   PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,  -- survives redelivery; the consumer's dedup key
    aggregate_id text        NOT NULL,         -- Kafka partition key
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz                   -- NULL = queued
);

CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
