create type job_status as enum ('pending', 'processing', 'finished', 'failed');

CREATE TABLE IF NOT EXISTS _jobs
(
  id          BIGSERIAL
    CONSTRAINT pk_jobs
      PRIMARY KEY,
  priority    SMALLINT                   NOT NULL,
  run_at      TIMESTAMP(0) DEFAULT now() NOT NULL,
  job_type    varchar(32)                NOT NULL,
  payload     BYTEA                      NOT NULL,
  metadata    BYTEA,
  error_count INTEGER                    NOT NULL DEFAULT 0,
  last_error  TEXT,
  queue       varchar(255)               NOT NULL,
  status      job_status                 NOT NULL DEFAULT 'pending',
  created_at  TIMESTAMP(0) DEFAULT now() NOT NULL,
  updated_at  TIMESTAMP(0) DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gue_jobs_selector ON _jobs (queue, status, run_at, priority);

