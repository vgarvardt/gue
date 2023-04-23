package gue

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/2tvenom/guex/database"
)

// Job is a single unit of work for Gue to perform.
type Job struct {
	database.Job

	processed int32
	db        *database.Queries
	backoff   Backoff
}

// Fail marks this job as failed
//
// You must also later call Done() to return this job's database connection to
// the pool. If you got the job from the worker - it will take care of cleaning up the job and resources,
// no need to do this manually in a WorkFunc.
func (j *Job) Fail(ctx context.Context) (err error) {
	return j.setProcessed(ctx, database.JobStatusFailed)
}

// Done commits transaction that marks job as done. If you got the job from the worker - it will take care of
// cleaning up the job and resources, no need to do this manually in a WorkFunc.
func (j *Job) Done(ctx context.Context) error {
	return j.setProcessed(ctx, database.JobStatusFinished)
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
	var (
		errorCount = j.ErrorCount + 1
		newRunAt   = j.calculateErrorRunAt(jErr, errorCount)
	)
	if newRunAt.IsZero() {
		return j.Fail(ctx)
	}

	return j.db.UpdateStatusError(ctx, database.UpdateStatusErrorParams{
		ID:         j.ID,
		ErrorCount: errorCount,
		RunAt:      newRunAt,
		LastError:  sql.NullString{String: jErr.Error(), Valid: true},
	})
}

func (j *Job) setProcessed(ctx context.Context, status database.JobStatus) (err error) {
	if atomic.LoadInt32(&j.processed) == 1 {
		return nil
	}

	if err = j.db.UpdateStatus(ctx, database.UpdateStatusParams{
		ID:     j.ID,
		Status: status,
	}); err != nil {
		return err
	}

	atomic.AddInt32(&j.processed, 1)
	return nil
}

func (j *Job) calculateErrorRunAt(err error, errorCount int32) time.Time {
	if errReschedule, ok := err.(ErrJobReschedule); ok {
		return errReschedule.rescheduleJobAt()
	}

	var backoff = j.backoff(int(errorCount))
	if backoff < 0 {
		return time.Time{}
	}

	return time.Now().UTC().Add(backoff)
}
