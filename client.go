package gue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/vgarvardt/gue/v5/adapter"
)

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

var (
	attrJobType = attribute.Key("job-type")
	attrSuccess = attribute.Key("success")
)

// Client is a Gue client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool    adapter.ConnPool
	logger  adapter.Logger
	id      string
	backoff Backoff
	meter   metric.Meter

	entropy io.Reader

	mEnqueue metric.Int64Counter
	mLockJob metric.Int64Counter
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool adapter.ConnPool, options ...ClientOption) (*Client, error) {
	instance := Client{
		pool:    pool,
		logger:  adapter.NoOpLogger{},
		id:      RandomStringID(),
		backoff: DefaultExponentialBackoff,
		meter:   noop.NewMeterProvider().Meter("noop"),
		entropy: &ulid.LockedMonotonicReader{
			MonotonicReader: ulid.Monotonic(rand.Reader, 0),
		},
	}

	for _, option := range options {
		option(&instance)
	}

	instance.logger = instance.logger.With(adapter.F("client-id", instance.id))

	return &instance, instance.initMetrics()
}

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(ctx context.Context, j *Job) error {
	return c.execEnqueue(ctx, j, c.pool)
}

// EnqueueWithID adds a job to the queue with a specific id
func (c *Client) EnqueueWithID(ctx context.Context, j *Job, ulid ulid.ULID) error {
	return c.execEnqueueWithID(ctx, j, c.pool, ulid)
}

// EnqueueTx adds a job to the queue within the scope of the transaction.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueTx(ctx context.Context, j *Job, tx adapter.Tx) error {
	return c.execEnqueue(ctx, j, tx)
}

// EnqueueBatch adds a batch of jobs. Operation is atomic, so either all jobs are added, or none.
func (c *Client) EnqueueBatch(ctx context.Context, jobs []*Job) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not begin transaction")
	}

	for i, j := range jobs {
		if err := c.execEnqueue(ctx, j, tx); err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				c.logger.Error("Could not properly rollback transaction", adapter.Err(err))
			}
			return fmt.Errorf("could not enqueue job from the batch [idx %d]: %w", i, err)
		}
	}

	return tx.Commit(ctx)
}

// EnqueueBatchTx adds a batch of jobs within the scope of the transaction.
// This allows you to guarantee that an enqueued batch will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueBatchTx(ctx context.Context, jobs []*Job, tx adapter.Tx) error {
	for i, j := range jobs {
		if err := c.execEnqueue(ctx, j, tx); err != nil {
			return fmt.Errorf("could not enqueue job from the batch [idx %d]: %w", i, err)
		}
	}

	return nil
}

func (c *Client) execEnqueueWithID(ctx context.Context, j *Job, q adapter.Queryable, jobID ulid.ULID) (err error) {
	if j.Type == "" {
		return ErrMissingType
	}

	j.CreatedAt = time.Now().UTC()

	runAt := j.RunAt
	if runAt.IsZero() {
		j.RunAt = j.CreatedAt
	}

	j.ID = jobID
	idAsString := jobID.String()

	if j.Args == nil {
		j.Args = []byte{}
	}

	_, err = q.Exec(ctx, `INSERT INTO gue_jobs
(job_id, queue, priority, run_at, job_type, args, skip_delete, status, created_at, updated_at)
VALUES
($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
`, idAsString, j.Queue, j.Priority, j.RunAt, j.Type, j.Args, boolToInt16(j.SkipDelete), JobStatusTodo, j.CreatedAt)

	c.logger.Debug(
		"Tried to enqueue a job",
		adapter.Err(err),
		adapter.F("queue", j.Queue),
		adapter.F("id", idAsString),
	)

	c.mEnqueue.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(err == nil)))

	return err
}

func (c *Client) execEnqueue(ctx context.Context, j *Job, q adapter.Queryable) error {
	jobID, err := ulid.New(ulid.Now(), c.entropy)
	if err != nil {
		return fmt.Errorf("could not generate new Job ULID ID: %w", err)
	}

	return c.execEnqueueWithID(ctx, j, q, jobID)
}

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If no job is found, nil will be returned instead of an error.
//
// This function cares about the priority first to lock top priority jobs first even if there are available ones that
// should be executed earlier but with the lower priority.
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting a job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *Client) LockJob(ctx context.Context, queue string) (*Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, skip_delete, status, error_count, last_error, created_at
FROM gue_jobs
WHERE queue = $1 AND run_at <= $2 AND (status = $3 OR status = $4)
ORDER BY priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`

	return c.execLockJob(ctx, true, sql, queue, time.Now().UTC(), JobStatusTodo, JobStatusRetry)
}

// LockJobByID attempts to retrieve a specific Job from the database.
// If the job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If the job is not found, an error will be returned
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting the job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *Client) LockJobByID(ctx context.Context, id ulid.ULID) (*Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, skip_delete, status, error_count, last_error, created_at
FROM gue_jobs
WHERE job_id = $1 FOR UPDATE SKIP LOCKED`

	return c.execLockJob(ctx, false, sql, id.String())
}

// LockNextScheduledJob attempts to retrieve the earliest scheduled Job from the database in the specified queue.
// If a job is found, it will be locked on the transactional level, so other workers
// will be skipping it. If no job is found, nil will be returned instead of an error.
//
// This function cares about the scheduled time first to lock earliest to execute jobs first even if there are ones
// with a higher priority scheduled to a later time but already eligible for execution
//
// Because Gue uses transaction-level locks, we have to hold the
// same transaction throughout the process of getting a job, working it,
// deleting it, and releasing the lock.
//
// After the Job has been worked, you must call either Job.Done() or Job.Error() on it
// in order to commit transaction to persist Job changes (remove or update it).
func (c *Client) LockNextScheduledJob(ctx context.Context, queue string) (*Job, error) {
	sql := `SELECT job_id, queue, priority, run_at, job_type, args, skip_delete, status, error_count, last_error, created_at
FROM gue_jobs
WHERE queue = $1 AND run_at <= $2 AND (status = $3 OR status = $4)
ORDER BY run_at, priority ASC
LIMIT 1 FOR UPDATE SKIP LOCKED`

	return c.execLockJob(ctx, true, sql, queue, time.Now().UTC(), JobStatusTodo, JobStatusRetry)
}

func (c *Client) execLockJob(ctx context.Context, handleErrNoRows bool, sql string, args ...any) (*Job, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		c.mLockJob.Add(ctx, 1, metric.WithAttributes(attrJobType.String(""), attrSuccess.Bool(false)))
		return nil, err
	}

	j := Job{tx: tx, backoff: c.backoff, logger: c.logger}

	var skipDelete int16
	err = tx.QueryRow(ctx, sql, args...).Scan(
		&j.ID,
		&j.Queue,
		&j.Priority,
		&j.RunAt,
		&j.Type,
		&j.Args,
		&skipDelete,
		&j.Status,
		&j.ErrorCount,
		&j.LastError,
		&j.CreatedAt,
	)
	if err == nil {
		j.SkipDelete = int16ToBool(skipDelete)
		c.mLockJob.Add(ctx, 1, metric.WithAttributes(attrJobType.String(j.Type), attrSuccess.Bool(true)))
		return &j, nil
	}

	rbErr := tx.Rollback(ctx)
	if handleErrNoRows && errors.Is(err, adapter.ErrNoRows) {
		return nil, rbErr
	}

	return nil, fmt.Errorf("could not lock a job (rollback result: %v): %w", rbErr, err)
}

func (c *Client) initMetrics() (err error) {
	if c.mEnqueue, err = c.meter.Int64Counter(
		"gue_client_enqueue",
		metric.WithDescription("Number of jobs being enqueued"),
		metric.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mEnqueue metric: %w", err)
	}

	if c.mLockJob, err = c.meter.Int64Counter(
		"gue_client_lock_job",
		metric.WithDescription("Number of jobs being locked (consumed)"),
		metric.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mLockJob metric: %w", err)
	}

	return nil
}

func boolToInt16(v bool) int16 {
	if v {
		return 1
	}
	return 0
}

func int16ToBool(v int16) bool {
	if v > 0 {
		return true
	}
	return false
}
