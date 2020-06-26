package gue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/vgarvardt/backoff"

	"github.com/vgarvardt/gue/adapter"
)

// Job is a single unit of work for Gue to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID int64

	// Queue is the name of the queue. It defaults to the empty queue "".
	Queue string

	// Priority is the priority of the Job. The default priority is 0, and a
	// lower number means a higher priority.
	//
	// The highest priority is -32768, the lowest one is +32767
	Priority int16

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type maps job to a worker func.
	Type string

	// Args must be the bytes of a valid JSON string
	Args []byte

	// ErrorCount is the number of times this job has attempted to run, but
	// failed with an error. It is ignored on job creation.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job
	// failed. It is ignored on job creation.
	LastError pgtype.Text

	mu      sync.Mutex
	deleted bool
	pool    adapter.ConnPool
	tx      adapter.Tx
}

// Tx returns DB transaction that this job is locked to. You may use
// it as you please until you call Done(). At that point, this transaction
// will be committed. This function will return nil if the Job's
// transaction was closed with Done().
func (j *Job) Tx() adapter.Tx {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.tx
}

// Delete marks this job as complete by deleting it form the database.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Delete(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.deleted {
		return nil
	}

	_, err := j.tx.Exec(ctx, `DELETE FROM gue_jobs WHERE job_id = $1`, j.ID)
	if err != nil {
		return err
	}

	j.deleted = true
	return nil
}

// Done commits transaction that marks job as done.
func (j *Job) Done(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.tx == nil || j.pool == nil {
		// already marked as done
		return
	}

	// TODO: log this error
	_ = j.tx.Commit(ctx)

	j.pool = nil
	j.tx = nil
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// This call marks job as done and releases (commits) transaction,
//so calling Done() is not required, although calling it will not cause any issues.
func (j *Job) Error(ctx context.Context, msg string) error {
	defer j.Done(ctx)

	errorCount := j.ErrorCount + 1

	backOff := backoff.Exponential{Config: backoff.Config{
		BaseDelay:  1.0 * time.Second,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   1.0 * time.Hour,
	}}
	delay := int(backOff.Backoff(int(errorCount)).Seconds())

	_, err := j.tx.Exec(ctx, `UPDATE gue_jobs
SET error_count = $1,
    run_at      = now() + $2 * '1 second'::interval,
    last_error  = $3,
	updated_at  = now()
WHERE job_id    = $4`, errorCount, delay, msg, j.ID)

	return err
}

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

	queue := &pgtype.Text{
		String: j.Queue,
		Status: pgtype.Null,
	}
	if j.Queue != "" {
		queue.Status = pgtype.Present
	}

	priority := &pgtype.Int2{
		Int:    j.Priority,
		Status: pgtype.Null,
	}
	if j.Priority != 0 {
		priority.Status = pgtype.Present
	}

	runAt := &pgtype.Timestamptz{
		Time:   j.RunAt,
		Status: pgtype.Null,
	}
	if !j.RunAt.IsZero() {
		runAt.Status = pgtype.Present
	}

	args := &pgtype.Bytea{
		Bytes:  j.Args,
		Status: pgtype.Null,
	}
	if len(j.Args) != 0 {
		args.Status = pgtype.Present
	}

	_, err := q.Exec(ctx, `INSERT INTO gue_jobs
(queue, priority, run_at, job_type, args)
VALUES
($1, $2, coalesce($3::timestamptz, now()::timestamptz), $4, coalesce($5::json, '[]'::json))
`, queue, priority, runAt, j.Type, args)

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

	err = tx.QueryRow(ctx, `SELECT queue, priority, run_at, job_id, job_type, args, error_count
FROM gue_jobs
WHERE queue = $1 AND run_at <= now()
ORDER BY priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`, queue).Scan(
		&j.Queue,
		&j.Priority,
		&j.RunAt,
		&j.ID,
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
