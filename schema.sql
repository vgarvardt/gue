CREATE TABLE IF NOT EXISTS gue_jobs
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

CREATE INDEX IF NOT EXISTS "idx_gue_jobs_selector" ON "gue_jobs" ("queue", "run_at", "priority");

COMMENT ON TABLE gue_jobs IS '1';
