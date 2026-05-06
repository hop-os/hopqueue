-- Schema v1. Applied once on first open. Future changes go in 0002_*.sql etc;
-- the storage layer applies any migration whose number exceeds the highest
-- already recorded in schema_migrations.

CREATE TABLE schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  INTEGER NOT NULL
);

CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,           -- format: '<entity>/<ulid>'
    lane            TEXT NOT NULL,
    durable_factor  INTEGER NOT NULL DEFAULT 0, -- 0=fire-and-forget, 1=any peer, 2=quorum, -1=all
    document        TEXT NOT NULL,              -- opaque JSON, never parsed by the queue
    state           TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    worker_id       TEXT,
    locked_until    INTEGER,
    available_at    INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    completed_at    INTEGER,                    -- set on terminal transitions, used for retention
    result          TEXT,
    error           TEXT,
    CHECK (state IN ('pending','processing','done','failed','released'))
);

CREATE INDEX idx_jobs_pending     ON jobs(lane, available_at) WHERE state = 'pending';
CREATE INDEX idx_jobs_processing  ON jobs(locked_until)        WHERE state = 'processing';
CREATE INDEX idx_jobs_completed   ON jobs(completed_at)        WHERE state IN ('done', 'failed');

-- replication_log carries every event a follower needs to apply, plus serves
-- as the SSE replay log. event_type uses the protocol's user-facing names
-- (enqueued/claimed/progress/done/failed/released/synced/deleted), so the
-- same row can be replayed straight to an SSE consumer or to a follower's
-- apply loop without translation.
CREATE TABLE replication_log (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type   TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    payload      TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    origin_node  TEXT NOT NULL,
    CHECK (event_type IN ('enqueued','claimed','progress','done','failed','released','synced','deleted'))
);

CREATE INDEX idx_replog_recent ON replication_log(seq DESC);
CREATE INDEX idx_replog_job    ON replication_log(job_id, seq);

CREATE TABLE node_state (
    node_id            TEXT PRIMARY KEY,
    current_seq        INTEGER NOT NULL DEFAULT 0,
    is_leader          INTEGER NOT NULL DEFAULT 0,
    last_lease_renewal INTEGER
);
