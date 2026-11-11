CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL   PRIMARY KEY,
    idempotency_key TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    -- fencing token: incremented on every claim, presented on every write by the owner
    attempt         INTEGER     NOT NULL DEFAULT 0,
    max_attempts    INTEGER     NOT NULL DEFAULT 5,
    worker_id       TEXT,
    lease_until     TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    CONSTRAINT jobs_idempotency_key_uniq UNIQUE (idempotency_key),
    CONSTRAINT jobs_running_has_lease CHECK ((status = 'running') = (lease_until IS NOT NULL)),
    CONSTRAINT jobs_terminal_has_finished
        CHECK ((status IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS jobs_queued_idx  ON jobs (id)          WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_until) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_status_idx  ON jobs (status, id);

-- One row every time a worker actually starts executing a job.
-- At-least-once: a job may have several rows here (killed / reaped / cancelled mid-run).
CREATE TABLE IF NOT EXISTS job_runs (
    id         BIGSERIAL   PRIMARY KEY,
    job_id     BIGINT      NOT NULL REFERENCES jobs (id),
    attempt    INTEGER     NOT NULL,
    worker_id  TEXT        NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_runs_job_idx ON job_runs (job_id);

-- The committed side effect, written in the same transaction as running -> succeeded.
-- Deliberately NO UNIQUE (job_id): the chaos harness must measure the protocol,
-- not a constraint that would turn a protocol bug into a quiet error.
CREATE TABLE IF NOT EXISTS job_effects (
    id         BIGSERIAL   PRIMARY KEY,
    job_id     BIGINT      NOT NULL REFERENCES jobs (id),
    attempt    INTEGER     NOT NULL,
    worker_id  TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_effects_job_idx ON job_effects (job_id);
