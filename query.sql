-- name: UpdateStatus :exec
UPDATE _jobs
SET status = $2
WHERE id = $1;

-- name: JobToProcessing :exec
UPDATE _jobs
SET status     = 'processing',
    updated_at = now(),
    run_at     = now()
WHERE id = ANY ($1::bigserial[]);

-- name: UpdateStatusError :exec
UPDATE _jobs
SET error_count = $1,
    run_at      = $2,
    last_error  = $3,
    updated_at  = now(),
    status      = 'pending'
WHERE id = $4;

-- name: RestoreStuck :exec
UPDATE _jobs
SET status = 'pending'
WHERE status = 'pending'
  AND queue = ANY ($1::varchar[])
  AND run_at <= now() - INTERVAL '' || $2 || ' minutes';

-- name: Enqueue :exec
INSERT INTO _jobs (queue, job_type, priority, run_at, payload, metadata)
VALUES ($1, $2, $3, $4, $5, $6)
returning id;

-- name: GetJob :one
SELECT *
FROM _jobs
WHERE id = $1
LIMIT 1;

-- name: GetTotalInfo :many
SELECT queue, status, count(*) as cnt
FROM _jobs
GROUP BY queue, status;

-- name: GetTasksByStatusAndQueueName :many
SELECT *
FROM _jobs
WHERE queue = $1
  AND status = ANY ($2::varchar[]::job_status[])
LIMIT $3 OFFSET $4;

-- name: GetTotalInfoByName :many
SELECT queue, status, count(*) as cnt, sum(error_count) as err_count
FROM _jobs
WHERE queue = $1
GROUP BY queue, status;

-- name: GetTotalInfoByNameHistory :many
SELECT queue, date(created_at), status, count(*) as cnt, sum(error_count) as err_count
FROM _jobs
WHERE queue = $1
  AND created_at > NOW() - INTERVAL '10 days'
GROUP BY queue, status, date(created_at)
ORDER BY date(created_at);

-- name: GetTotalInfoHistory :many
SELECT queue, date(created_at), status, count(*) as cnt, sum(error_count) as err_count
FROM _jobs
WHERE created_at > NOW() - INTERVAL '90 days'
GROUP BY queue, status, date(created_at)
ORDER BY date(created_at);

-- name: GetTotalInfoWithErrors :many
SELECT queue, count(*) as cnt
FROM _jobs
WHERE error_count > 0
  AND status = 'pending'
GROUP BY queue;
