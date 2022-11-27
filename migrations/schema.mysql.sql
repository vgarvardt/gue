CREATE TABLE IF NOT EXISTS gue_jobs
(
  job_id      VARCHAR(27)  NOT NULL PRIMARY KEY,
  priority    SMALLINT     NOT NULL,
  run_at      DATETIME     NOT NULL,
  job_type    VARCHAR(128) NOT NULL,
  args        BLOB         NOT NULL,
  error_count INTEGER      NOT NULL DEFAULT 0,
  last_error  TEXT,
  queue       VARCHAR(128) NOT NULL,
  created_at  DATETIME     NOT NULL,
  updated_at  DATETIME     NOT NULL,
  INDEX (queue, run_at, priority)
);
