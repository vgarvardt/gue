package gue

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/2tvenom/gue/adapter"
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

// Job is a single unit of work for Gue to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID int64

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

	// ErrorCount is the number of times this job has attempted to run, but failed with an error.
	// It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job handler errored.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job failed. It is ignored on job creation.
	// This field is initialised only when the Job is being retrieved from the DB and is not
	// being updated when the current Job run errored. This field supposed to be used mostly for the debug reasons.
	LastError sql.NullString

	mu        sync.Mutex
	processed bool
	db        adapter.ConnPool
	backoff   Backoff
	logger    adapter.Logger
}

// Fail marks this job as failed
//
// You must also later call Done() to return this job's database connection to
// the pool. If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *Job) Fail(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.processed {
		return nil
	}

	_, err := j.db.Exec(ctx, `UPDATE _jobs SET status='failed' WHERE id = $1`, j.ID)
	if err != nil {
		return err
	}

	j.processed = true
	return nil
}

// Done commits transaction that marks job as done. If you got the job from the worker - it will take care of
// cleaning up the job and resources, no need to do this manually in a WorkFunc.
func (j *Job) Done(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.db == nil {
		return nil
	}

	if _, err := j.db.Exec(ctx, `UPDATE _jobs SET status='finished', updated_at=now() WHERE id = $1`, j.ID); err != nil {
		return err
	}

	j.db = nil

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
	errorCount := j.ErrorCount + 1
	newRunAt := j.calculateErrorRunAt(jErr, time.Now().UTC(), errorCount)
	if newRunAt.IsZero() {
		j.logger.Info(
			"Got empty new run at for the errored job, discarding it",
			adapter.F("job-type", j.Type),
			adapter.F("job-queue", j.Queue),
			adapter.F("job-errors", errorCount),
			adapter.Err(jErr),
		)
		err = j.Fail(ctx)
		return
	}

	_, err = j.db.Exec(
		ctx,
		`UPDATE _jobs SET error_count = $1, run_at = $2, last_error = $3, updated_at = now(), status = 'pending' WHERE id = $4`,
		errorCount, newRunAt, jErr.Error(), j.ID,
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
