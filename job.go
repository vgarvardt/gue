package gue

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/sadpenguinn/gue/v6/adapter"
)

// JobPriority is the wrapper type for Job.Priority
type JobPriority int16

// Some shortcut values for JobPriority that can be any, but chances are high that one of these will be the most used.
const (
	JobPriorityHighest JobPriority = -32768
	JobPriorityHigh    JobPriority = -16384
	JobPriorityDefault JobPriority = 0
	JobPriorityLow     JobPriority = 16384
	JobPriorityLowest  JobPriority = 32767
)

type JobStatus string

const (
	JobStatusTodo    JobStatus = "todo"
	JobStatusRetry   JobStatus = "retry"
	JobStatusSuccess JobStatus = "success"
	JobStatusError   JobStatus = "error"
)

// Job is a single unit of work for Gue to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID ulid.ULID

	// Queue is the name of the queue. It defaults to the empty queue "".
	Queue string

	// Priority is the priority of the Job. The default priority is 0, and a
	// lower number means a higher priority.
	//
	// The highest priority is JobPriorityHighest, the lowest one is JobPriorityLowest
	Priority JobPriority

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type maps job to a worker func.
	Type string

	// Args for the job.
	Args []byte

	// SkipDelete is it necessary to keep task in postgres queue after successful execution.
	// If this field set to true, gue won't delete task from postgres table.
	SkipDelete bool

	// JobStatus is the task execution status.
	// It is ignored on job creation.
	Status JobStatus

	// ErrorCount is the number of times this job has attempted to run, but failed with an error.
	// It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job handler errored.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job failed. It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job run errored. This field supposed to be used mostly for the debug reasons.
	LastError sql.NullString

	// CreatedAt is the job creation time.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job run errored. This field can be used as a decision parameter in some handlers
	// whether it makes sense to retry the job or it can be dropped.
	CreatedAt time.Time

	mu       sync.Mutex
	finished bool
	tx       adapter.Tx
	backoff  Backoff
	logger   adapter.Logger
}

// Tx returns DB transaction that this job is locked to. You may use
// it as you please until you call Done(). At that point, this transaction
// will be committed. This function will return nil if the Job's
// transaction was closed with Done().
func (j *Job) Tx() adapter.Tx {
	return j.tx
}

// Finish marks this job as complete.
// If job has property SkipDelete set to true, then Finish function will mark it as finished by changing its status field.
// If job has no such property, it will mark job as complete by deleting it from the database.
//
// You must also later call Done() to return this job's database connection to
// the pool. If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *Job) Finish(ctx context.Context, status JobStatus) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.finished {
		return nil
	}

	var err error
	if j.SkipDelete {
		_, err = j.tx.Exec(ctx, `UPDATE gue_jobs SET status = $1 WHERE job_id = $2`, status, j.ID.String())
	} else {
		_, err = j.tx.Exec(ctx, `DELETE FROM gue_jobs WHERE job_id = $1`, j.ID.String())
	}

	if err != nil {
		return err
	}

	j.finished = true
	return nil
}

// Done commits transaction that marks job as done. If you got the job from the worker - it will take care of
// cleaning up the job and resources, no need to do this manually in a WorkFunc.
func (j *Job) Done(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.tx == nil {
		// already marked as done
		return nil
	}

	if err := j.tx.Commit(ctx); err != nil {
		return err
	}

	j.tx = nil

	return nil
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// This call marks job as done and releases (commits) transaction,
// so calling Done() is not required, although calling it will not cause any issues.
// If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *Job) Error(ctx context.Context, jErr error) (err error) {
	defer func() {
		doneErr := j.Done(ctx)
		if doneErr != nil {
			err = fmt.Errorf("failed to mark job as done (original error: %v): %w", err, doneErr)
		}
	}()

	errorCount := j.ErrorCount + 1
	now := time.Now().UTC()
	newRunAt := j.calculateErrorRunAt(jErr, now, errorCount)
	if newRunAt.IsZero() {
		j.logger.Info(
			"Got empty new run at for the errored job, discarding it",
			adapter.F("job-type", j.Type),
			adapter.F("job-queue", j.Queue),
			adapter.F("job-errors", errorCount),
			adapter.Err(jErr),
		)

		// save last error for history
		_, err = j.tx.Exec(
			ctx,
			`UPDATE gue_jobs SET error_count = $1, last_error = $2, updated_at = $3 WHERE job_id = $4`,
			errorCount, jErr.Error(), now, j.ID.String(),
		)
		if err != nil {
			return
		}

		err = j.Finish(ctx, JobStatusError)
		return
	}

	_, err = j.tx.Exec(
		ctx,
		`UPDATE gue_jobs SET error_count = $1, run_at = $2, last_error = $3, updated_at = $4, status = $5 WHERE job_id = $6`,
		errorCount, newRunAt, jErr.Error(), now, JobStatusRetry, j.ID.String(),
	)

	return err
}

func (j *Job) calculateErrorRunAt(err error, now time.Time, errorCount int32) time.Time {
	errReschedule, ok := err.(ErrJobReschedule)
	if ok {
		return errReschedule.rescheduleJobAt()
	}

	backoff := j.backoff(int(errorCount))
	if backoff < 0 {
		return time.Time{}
	}

	return now.Add(backoff)
}
