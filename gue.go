package gue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vgarvardt/gue/adapter"
)

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

// Client is a Que client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool adapter.ConnPool

	// TODO: add options for default queueing options, logger and table name
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool adapter.ConnPool) *Client {
	return &Client{pool: pool}
}

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(ctx context.Context, j *Job) error {
	return execEnqueue(ctx, j, c.pool)
}

// EnqueueInTx adds a job to the queue within the scope of the transaction tx.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueInTx(ctx context.Context, j *Job, tx adapter.Tx) error {
	return execEnqueue(ctx, j, tx)
}

func execEnqueue(ctx context.Context, j *Job, q adapter.Queryable) error {
	if j.Type == "" {
		return ErrMissingType
	}

	runAt := j.RunAt
	if runAt.IsZero() {
		runAt = time.Now()
	}

	if len(j.Args) == 0 {
		j.Args = []byte(`[]`)
	}

	err := q.QueryRow(ctx, `INSERT INTO gue_jobs
(queue, priority, run_at, job_type, args, created_at, updated_at)
VALUES
($1, $2, $3, $4, $5, $6, $6) RETURNING job_id
`, j.Queue, j.Priority, runAt, j.Type, j.Args, time.Now()).Scan(&j.ID)

	return err
}

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, a session-level Postgres advisory lock is created for the
// Job's ID. If no job is found, nil will be returned instead of an error.
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting a job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Done() or Error() on it
// in order to return the database connection to the pool and remove the lock.
func (c *Client) LockJob(ctx context.Context, queue string) (*Job, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}

	j := Job{pool: c.pool, tx: tx}

	err = tx.QueryRow(ctx, `SELECT job_id, queue, priority, run_at, job_type, args, error_count
FROM gue_jobs
WHERE queue = $1 AND run_at <= $2
ORDER BY priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`, queue, time.Now()).Scan(
		&j.ID,
		&j.Queue,
		&j.Priority,
		&j.RunAt,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
	)
	if err == nil {
		return &j, nil
	}

	rbErr := tx.Rollback(ctx)
	if err == adapter.ErrNoRows {
		return nil, rbErr
	}

	return nil, fmt.Errorf("could not lock a job (rollback result: %v): %w", rbErr, err)
}
