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
FROM _jobs WHERE queue = $1 AND status = ANY($2::varchar[]::job_status[])
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
