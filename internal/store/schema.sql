-- EventMesh schema. Applied at startup; every statement is idempotent.

CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    -- The UNIQUE constraint is what enforces idempotency. Two concurrent
    -- inserts with the same key cannot both succeed, no matter how they race.
    idempotency_key TEXT        NOT NULL UNIQUE,
    queue           TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL
                    CHECK (status IN ('pending', 'running', 'retrying', 'succeeded', 'dead')),
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL CHECK (max_attempts >= 1),
    last_error      TEXT,
    -- When the API received the request; the start of end-to-end latency.
    received_at     TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    -- Earliest time a retrying job may run again (backoff).
    run_at          TIMESTAMPTZ NOT NULL,
    -- Visibility timeout: a running job whose lease has passed may be reclaimed.
    lease_until     TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);

-- Only unfinished jobs are scanned by the sweeper, so index only those.
CREATE INDEX IF NOT EXISTS jobs_unfinished_idx ON jobs (queue, status)
    WHERE status IN ('pending', 'running', 'retrying');

CREATE INDEX IF NOT EXISTS jobs_dead_idx ON jobs (id) WHERE status = 'dead';
