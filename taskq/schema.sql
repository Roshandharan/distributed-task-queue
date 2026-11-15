-- PostgreSQL is the source of truth for every job. Redis only holds the
-- dispatch order, and can be rebuilt from this table at any time.

CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY,
    queue             TEXT        NOT NULL DEFAULT 'default',
    task              TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    priority          SMALLINT    NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
    status            TEXT        NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
    attempts          INTEGER     NOT NULL DEFAULT 0,
    max_attempts      INTEGER     NOT NULL DEFAULT 5,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at  TIMESTAMPTZ,
    worker_id         TEXT,
    idempotency_key   TEXT UNIQUE,
    result            JSONB,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ
);

-- Reaper: find running jobs whose lease ran out.
CREATE INDEX IF NOT EXISTS jobs_running_lease_idx
    ON jobs (lease_expires_at) WHERE status = 'running';
-- Reconciler: walk queued jobs in id order.
CREATE INDEX IF NOT EXISTS jobs_queued_idx
    ON jobs (id) WHERE status = 'queued';
-- Dead-letter queue listing.
CREATE INDEX IF NOT EXISTS jobs_dead_idx
    ON jobs (finished_at DESC) WHERE status = 'dead';

-- One row per failed attempt, lease expiry, or dead-lettering.
CREATE TABLE IF NOT EXISTS job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    event      TEXT        NOT NULL,
    attempt    INTEGER     NOT NULL,
    worker_id  TEXT,
    detail     TEXT,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);
