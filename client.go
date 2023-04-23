package gue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/2tvenom/guex/database"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"
)

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

type QueueLimit struct {
	Queue string
	Limit int32
}

// Client is a Gue client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool    *database.Queries
	id      string
	backoff Backoff
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool *database.Queries, options ...ClientOption) (c *Client, err error) {
	c = &Client{
		pool:    pool,
		backoff: DefaultExponentialBackoff,
	}

	for _, option := range options {
		option(c)
	}

	return c, nil
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
func (c *Client) EnqueueTx(ctx context.Context, j *Job, tx pgx.Tx) error {
	return c.execEnqueue(ctx, j, c.pool.WithTx(tx))
}

// EnqueueBatch adds a batch of jobs. Operation is atomic, so either all jobs are added, or none.
func (c *Client) EnqueueBatch(ctx context.Context, jobs []*Job) error {
	err := c.pool.Pool().BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return c.EnqueueBatchTx(ctx, jobs, tx)
	})
	if err != nil {
		return fmt.Errorf("could not make batch enqueue: %w", err)
	}
	return nil
}

// EnqueueBatchTx adds a batch of jobs within the scope of the transaction.
// This allows you to guarantee that an enqueued batch will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueBatchTx(ctx context.Context, jobs []*Job, tx pgx.Tx) error {
	for _, j := range jobs {
		if err := c.execEnqueue(ctx, j, c.pool.WithTx(tx)); err != nil {
			return fmt.Errorf("could not enqueue job from the batch: %w", err)
		}
	}

	return nil
}

func (c *Client) execEnqueue(ctx context.Context, j *Job, q *database.Queries) (err error) {
	if j.JobType == "" {
		return ErrMissingType
	}

	if j.RunAt.IsZero() {
		j.RunAt = time.Now().UTC().Add(-time.Second)
	} else {
		j.RunAt = j.RunAt.UTC()
	}

	if j.Payload == nil {
		j.Payload = []byte{}
	}

	var query = q
	if query != nil {
		query = c.pool
	}

	err = query.Enqueue(ctx, database.EnqueueParams{
		Queue:    j.Queue,
		JobType:  j.JobType,
		Priority: j.Priority,
		RunAt:    j.RunAt,
		Payload:  j.Payload,
		Metadata: j.Metadata,
	})

	EnqueueMeter.Add(ctx, 1, attribute.String("type", j.JobType), attribute.String("queue", j.Queue))

	return err
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
func (c *Client) LockNextScheduledJob(ctx context.Context, limits []QueueLimit) (jobs []*Job, err error) {
	var (
		vals string
		args []interface{}
	)
	for i, l := range limits {
		if l.Limit <= 0 {
			continue
		}
		if vals != "" {
			vals += ","
		}
		vals += fmt.Sprintf("($%d, $%d)", (i*2)+1, (i*2)+2)
		args = append(args, l.Queue, l.Limit)
	}

	if len(args) == 0 {
		return nil, nil
	}

	var (
		query = `WITH limits AS (SELECT *
							FROM (VALUES %s) AS code(queue, lim)),
				pending AS (SELECT id,
									queue,
									row_number() OVER (PARTITION BY queue ORDER BY run_at, priority ASC) AS r
							 FROM _jobs j
							 WHERE status = 'pending'),
				ids AS (SELECT *
						 FROM pending p
								  INNER JOIN limits l ON p.queue = l.queue AND p.r <= l.lim)
				SELECT j.id,
					   j.queue,
					   j.status,
					   j.priority,
					   j.run_at,
					   j.job_type,
					   j.payload,
					   j.metadata,
					   j.error_count,
					   j.last_error,
					   j.created_at,
					   j.updated_at
				FROM _jobs j INNER JOIN ids i ON j.id = i.id FOR UPDATE SKIP LOCKED;`
	)

	err = c.pool.Pool().BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var rows pgx.Rows
		if rows, err = tx.Query(ctx, fmt.Sprintf(query, vals), args...); err != nil {
			return fmt.Errorf("error get jobs: %w", err)
		}

		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var job = &Job{backoff: c.backoff, db: c.pool}
			if err = rows.Scan(
				&job.ID,
				&job.Queue,
				&job.Status,
				&job.Priority,
				&job.RunAt,
				&job.JobType,
				&job.Payload,
				&job.Metadata,
				&job.ErrorCount,
				&job.LastError,
				&job.CreatedAt,
				&job.UpdatedAt,
			); err != nil {
				return fmt.Errorf("error scan job: %w", err)
			}
			ids = append(ids, job.ID)
			jobs = append(jobs, job)
		}

		if err = c.pool.WithTx(tx).JobToProcessing(ctx, ids); err != nil {
			return fmt.Errorf("error set jobs to status processing: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return jobs, nil
}

func (c *Client) RestoreStuck(ctx context.Context, runAfter time.Duration, queue ...QueueLimit) (err error) {
	var queues = make([]string, len(queue))
	for i, q := range queue {
		queues[i] = q.Queue
	}

	return c.pool.RestoreStuck(ctx, database.RestoreStuckParams{
		Column1: queues,
		Column2: sql.NullString{String: fmt.Sprintf("%d", int(runAfter.Minutes())), Valid: true},
	})
}
