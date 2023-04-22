package gue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlexanderGrom/qb"
	"github.com/vgarvardt/gue/v5/adapter"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
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

	mEnqueue instrument.Int64Counter
	mLockJob instrument.Int64Counter
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool adapter.ConnPool, options ...ClientOption) (*Client, error) {
	instance := Client{
		pool:    pool,
		logger:  adapter.NoOpLogger{},
		id:      RandomStringID(),
		backoff: DefaultExponentialBackoff,
		meter:   metric.NewNoopMeterProvider().Meter("noop"),
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

func (c *Client) execEnqueue(ctx context.Context, j *Job, q adapter.Queryable) (err error) {
	if j.Type == "" {
		return ErrMissingType
	}

	if j.RunAt.IsZero() {
		j.RunAt = time.Now().UTC().Add(-time.Second)
	} else {
		j.RunAt = j.RunAt.UTC()
	}

	if j.Args == nil {
		j.Args = []byte{}
	}

	var query = q
	if query != nil {
		query = c.pool
	}

	err = query.QueryRow(
		ctx,
		`INSERT INTO _jobs (queue, priority, run_at, job_type, args) VALUES ($1, $2, $3, $4, $5) returning id`,
		j.Queue,
		j.Priority,
		j.RunAt,
		j.Type,
		j.Args,
	).Scan(&j.ID)

	j.db = c.pool

	c.logger.Debug(
		"Tried to enqueue a job",
		adapter.Err(err),
		adapter.F("queue", j.Queue),
	)

	c.mEnqueue.Add(ctx, 1, attrJobType.String(j.Type), attrSuccess.Bool(err == nil))

	return err
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
func (c *Client) LockJob(ctx context.Context, queue ...string) (*Job, error) {
	var (
		sql = `SELECT id, queue, priority, run_at, job_type, args, error_count, last_error
				FROM _jobs
				WHERE %s
				ORDER BY priority ASC LIMIT 1 FOR UPDATE SKIP LOCKED
			`
		w = new(qb.WhereBuilder).
			Where("run_at", "<=", time.Now().UTC()).
			Where("status", "=", "pending")
	)

	if len(queue) > 0 {
		w.WhereIn("queue", ToInterfaceSlice(queue)...)
	}
	var q = qb.Query(sql, w)

	return c.execLockJob(ctx, true, q.String(), q.Params()...)
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
func (c *Client) LockJobByID(ctx context.Context, id int64) (*Job, error) {
	var (
		sql = `SELECT id, queue, priority, run_at, job_type, args, error_count, last_error
				FROM _jobs
				WHERE %s
				FOR UPDATE SKIP LOCKED
			`
		w = new(qb.WhereBuilder).
			Where("id", "=", id).
			Where("status", "=", "pending")
		q = qb.Query(sql, w)
	)
	return c.execLockJob(ctx, false, q.String(), q.Params()...)
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
func (c *Client) LockNextScheduledJob(ctx context.Context, queue ...string) (*Job, error) {
	var (
		sql = `SELECT id, queue, priority, run_at, job_type, args, error_count, last_error
				FROM _jobs
				WHERE %s
				ORDER BY run_at, priority ASC LIMIT 1 FOR UPDATE SKIP LOCKED
			`
		w = new(qb.WhereBuilder).
			Where("run_at", "<=", time.Now().UTC()).
			Where("status", "=", "pending")
	)

	if len(queue) > 0 {
		w.WhereIn("queue", ToInterfaceSlice(queue)...)
	}
	var q = qb.Query(sql, w)

	return c.execLockJob(ctx, true, q.String(), q.Params()...)
}

func (c *Client) CleanUp(ctx context.Context, runAfter time.Duration, queue ...string) (err error) {
	var (
		sql = `UPDATE _jobs SET status = 'pending' WHERE %s`
		w   = new(qb.WhereBuilder).
			Where("status", "=", "pending").
			WhereIn("queue", ToInterfaceSlice(queue)...).
			Where("run_at", "<=", fmt.Sprintf("now() - INTERVAL '%d minutes", int(runAfter.Minutes())))
		q = qb.Query(sql, w)
	)

	if _, err = c.pool.Exec(ctx, q.String(), q.Params()...); err != nil {
		return fmt.Errorf("error cleanup job table: %w", err)
	}
	return nil
}

func (c *Client) execLockJob(ctx context.Context, handleErrNoRows bool, sql string, args ...any) (j *Job, err error) {
	var tx adapter.Tx
	if tx, err = c.pool.Begin(ctx); err != nil {
		c.mLockJob.Add(ctx, 1, attrJobType.String(""), attrSuccess.Bool(false))
		return nil, err
	}

	j = &Job{backoff: c.backoff, logger: c.logger, db: c.pool}

	err = func() error {
		err = tx.QueryRow(ctx, sql, args...).Scan(
			&j.ID,
			&j.Queue,
			&j.Priority,
			&j.RunAt,
			&j.Type,
			&j.Args,
			&j.ErrorCount,
			&j.LastError,
		)

		if err != nil {
			return fmt.Errorf("error get job from _job: %w", err)
		}

		if _, err = tx.Exec(ctx, "UPDATE _jobs SET status='processing', updated_at=now() WHERE id=$1", j.ID); err != nil {
			return fmt.Errorf("error set status processing for job %d: %w", j.ID, err)
		}

		return nil
	}()

	if err == nil {
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("error commit transaction: %w", err)
		}

		c.mLockJob.Add(ctx, 1, attrJobType.String(j.Type), attrSuccess.Bool(true))
		return j, nil
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
		instrument.WithDescription("Number of jobs being enqueued"),
		instrument.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mEnqueue metric: %w", err)
	}

	if c.mLockJob, err = c.meter.Int64Counter(
		"gue_client_lock_job",
		instrument.WithDescription("Number of jobs being locked (consumed)"),
		instrument.WithUnit("1"),
	); err != nil {
		return fmt.Errorf("could not register mLockJob metric: %w", err)
	}

	return nil
}

func ToInterfaceSlice[T any](slice []T) []interface{} {
	var o = make([]interface{}, len(slice))
	for i := range slice {
		o[i] = slice[i]
	}
	return o
}
