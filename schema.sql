CREATE TABLE IF NOT EXISTS que_jobs
(
    job_id      bigserial   NOT NULL PRIMARY KEY,
    priority    smallint    NOT NULL DEFAULT 0,
    run_at      timestamptz NOT NULL DEFAULT now(),
    job_type    text        NOT NULL,
    args        json        NOT NULL DEFAULT '[]'::json,
    error_count integer     NOT NULL DEFAULT 0,
    last_error  text,
    queue       text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "idx_que_jobs_selector" ON "que_jobs" ("queue", "run_at", "priority");

COMMENT ON TABLE que_jobs IS '1';
