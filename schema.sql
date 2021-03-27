CREATE SCHEMA IF NOT EXISTS "{{.Schema}}";

CREATE TABLE IF NOT EXISTS "{{.Schema}}".gue_jobs
(
    job_id      bigserial   NOT NULL PRIMARY KEY,
    priority    smallint    NOT NULL,
    run_at      timestamptz NOT NULL,
    job_type    text        NOT NULL,
    args        json        NOT NULL,
    error_count integer     NOT NULL DEFAULT 0,
    last_error  text,
    queue       text        NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS "idx_gue_jobs_selector" ON "{{.Schema}}"."gue_jobs" ("queue", "run_at", "priority");

COMMENT ON TABLE "{{.Schema}}".gue_jobs IS '1';
