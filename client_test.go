package gue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	adapterTesting "github.com/sadpenguinn/gue/v6/adapter/testing"
)

func TestLockJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJob(t, openFunc(t))
		})
	}
}

func testLockJob(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{
		Type: "MyJob",
		Args: []byte(`{invalid]json>`),
	}
	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.NotEmpty(t, newJob.ID)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	require.NotNil(t, j.tx)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID.String(), j.ID.String())
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(`{invalid]json>`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.False(t, j.LastError.Valid)
}

func TestLockJobAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	j2, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j2)
}

func TestLockJobNoJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobNoJob(t, openFunc(t))
		})
	}
}

func testLockJobNoJob(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestLockJobCustomQueue(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobCustomQueue(t, openFunc(t))
		})
	}
}

func testLockJobCustomQueue(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob", Queue: "extra_priority"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)

	j, err = c.LockJob(ctx, "extra_priority")
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	err = j.Finish(ctx, JobStatusSuccess)
	require.NoError(t, err)
}

func TestLockJobByID(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobByID(t, openFunc(t))
		})
	}
}

func testLockJobByID(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{
		Type: "MyJob",
	}
	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.NotEmpty(t, newJob.ID)

	j, err := c.LockJobByID(ctx, newJob.ID)
	require.NoError(t, err)

	require.NotNil(t, j.tx)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID.String(), j.ID.String())
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(``), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.False(t, j.LastError.Valid)
}

func TestLockJobByIDAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobByIDAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockJobByIDAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{
		Type: "MyJob",
	}

	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, newJob.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	j2, err := c.LockJobByID(ctx, newJob.ID)
	require.Error(t, err)
	require.Nil(t, j2)
}

func TestLockJobByIDNoJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobByIDNoJob(t, openFunc(t))
		})
	}
}

func testLockJobByIDNoJob(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, ulid.Make())
	require.Error(t, err)
	require.Nil(t, j)
}

func TestLockNextScheduledJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockNextScheduledJob(t, openFunc(t))
		})
	}
}

func testLockNextScheduledJob(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{
		Type:  "MyJob",
		RunAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.NotEmpty(t, newJob.ID)

	j, err := c.LockNextScheduledJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID.String(), j.ID.String())
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(``), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.False(t, j.LastError.Valid)
}

func TestLockNextScheduledJobAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockNextScheduledJobAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockNextScheduledJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockNextScheduledJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	j2, err := c.LockNextScheduledJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j2)
}

func TestLockNextScheduledJobNoJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockNextScheduledJobNoJob(t, openFunc(t))
		})
	}
}

func testLockNextScheduledJobNoJob(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	j, err := c.LockNextScheduledJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestJobTx(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobTx(t, openFunc(t))
		})
	}
}

func testJobTx(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, j.tx, j.Tx())
}

func TestJobConnRace(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobConnRace(t, openFunc(t))
		})
	}
}

func testJobConnRace(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	var wg sync.WaitGroup
	wg.Add(2)

	// call Tx and Done in different goroutines to make sure they are safe from races
	go func() {
		_ = j.Tx()
		wg.Done()
	}()
	go func() {
		err := j.Done(ctx)
		assert.NoError(t, err)

		wg.Done()
	}()
	wg.Wait()
}

func TestJobFinish(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobFinish(t, openFunc(t))
		})
	}
}

func testJobFinish(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Finish(ctx, JobStatusSuccess)
	require.NoError(t, err)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure job was deleted
	jj, err := c.LockJobByID(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrNoRows))
	assert.Nil(t, jj)
}

func TestJobFinishSkipDelete(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobFinishSkipDelete(t, openFunc(t))
		})
	}
}

func testJobFinishSkipDelete(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	job := Job{Type: "MyJob", SkipDelete: true}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Finish(ctx, JobStatusSuccess)
	require.NoError(t, err)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure job was not deleted
	j2, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, job.Type, j2.Type)
	assert.Equal(t, JobStatusSuccess, j2.Status)
}

func TestJobDone(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobDone(t, openFunc(t))
		})
	}
}

func testJobDone(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure tx was cleared
	assert.Nil(t, j.tx)
}

func TestJobDoneMultiple(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobDoneMultiple(t, openFunc(t))
		})
	}
}

func testJobDoneMultiple(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Done(ctx)
	require.NoError(t, err)
	// try calling Done() again
	err = j.Done(ctx)
	require.NoError(t, err)
}

func TestJobError(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobError(t, openFunc(t))
		})
	}
}

func testJobError(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	job := &Job{Type: "MyJob"}
	err = c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, errors.New(msg))
	require.NoError(t, err)

	// make sure job was not deleted
	j2, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	})

	assert.True(t, j2.LastError.Valid)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
	assert.Equal(t, JobStatusRetry, j2.Status)
}

func TestJobErrorSkipDelete(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobErrorSkipDelete(t, openFunc(t))
		})
	}
}

func testJobErrorSkipDelete(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	job := &Job{Type: "MyJob", SkipDelete: true}
	err = c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, errors.New(msg))
	require.NoError(t, err)

	// make sure job was not deleted
	j2, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	})

	assert.True(t, j2.LastError.Valid)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
	assert.Equal(t, JobStatusRetry, j2.Status)
}

func TestJobErrorCustomBackoff(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobErrorCustomBackoff(t, openFunc(t))
		})
	}
}

func testJobErrorCustomBackoff(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	customBackoff := func(retries int) time.Duration {
		return time.Duration(retries) * time.Hour
	}

	c, err := NewClient(connPool, WithClientBackoff(customBackoff))
	require.NoError(t, err)

	job := &Job{Type: "MyJob"}
	err = c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, errors.New(msg))
	require.NoError(t, err)

	// make sure job was not deleted
	j2, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	})

	assert.True(t, j2.LastError.Valid)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
	// a diff in a sec is possible when doing dates math, so allow it
	assert.WithinDuration(t, job.RunAt.Add(time.Hour), j2.RunAt, time.Second)
	assert.Equal(t, JobStatusRetry, j2.Status)
}

func TestJobErrorBackoffNever(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobErrorBackoffNever(t, openFunc(t))
		})
	}
}

func testJobErrorBackoffNever(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool, WithClientBackoff(BackoffNever))
	require.NoError(t, err)

	job := &Job{Type: "MyJob"}
	err = c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, errors.New(msg))
	require.NoError(t, err)

	// make sure job was deleted
	jj, err := c.LockJobByID(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrNoRows))
	assert.Nil(t, jj)
}

func TestJobErrorBackoffNeverSkipDelete(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobErrorBackoffNeverSkipDelete(t, openFunc(t))
		})
	}
}

func testJobErrorBackoffNeverSkipDelete(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool, WithClientBackoff(BackoffNever))
	require.NoError(t, err)

	job := &Job{Type: "MyJob", SkipDelete: true}
	err = c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, errors.New(msg))
	require.NoError(t, err)

	// make sure job was not deleted
	j2, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	})

	assert.True(t, j2.LastError.Valid)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Equal(t, j2.RunAt.Unix(), job.RunAt.Unix())
	assert.Equal(t, JobStatusError, j2.Status)
}

func TestJobPriority(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobPriority(t, openFunc(t))
		})
	}
}

func testJobPriority(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	// insert in the order different from expected to be locked
	jobPriorityDefault := &Job{Type: "MyJob", Priority: JobPriorityDefault, Args: []byte(`default`)}
	err = c.Enqueue(ctx, jobPriorityDefault)
	require.NoError(t, err)

	jobPriorityLowest := &Job{Type: "MyJob", Priority: JobPriorityLowest, Args: []byte(`lowest`)}
	err = c.Enqueue(ctx, jobPriorityLowest)
	require.NoError(t, err)

	jobPriorityHighest := &Job{Type: "MyJob", Priority: JobPriorityHighest, Args: []byte(`highest`)}
	err = c.Enqueue(ctx, jobPriorityHighest)
	require.NoError(t, err)

	jobPriorityLow := &Job{Type: "MyJob", Priority: JobPriorityLow, Args: []byte(`low`)}
	err = c.Enqueue(ctx, jobPriorityLow)
	require.NoError(t, err)

	jobPriorityHigh := &Job{Type: "MyJob", Priority: JobPriorityHigh, Args: []byte(`high`)}
	err = c.Enqueue(ctx, jobPriorityHigh)
	require.NoError(t, err)

	expectedOrder := []*Job{jobPriorityHighest, jobPriorityHigh, jobPriorityDefault, jobPriorityLow, jobPriorityLowest}
	for _, expected := range expectedOrder {
		j, err := c.LockJob(ctx, "")
		require.NoError(t, err)
		require.NotNil(t, j)
		t.Cleanup(func() {
			err := j.Done(ctx)
			assert.NoError(t, err)
		})

		assert.Equal(t, expected.Priority, j.Priority)
		assert.Equal(t, expected.Args, j.Args)
	}
}

func findOneJob(t testing.TB, q adapter.Queryable) *Job {
	t.Helper()

	j := new(Job)
	err := q.QueryRow(
		context.Background(),
		`SELECT priority, run_at, job_id, job_type, args, error_count, last_error, queue, created_at FROM gue_jobs LIMIT 1`,
	).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
		&j.CreatedAt,
	)
	if errors.Is(err, adapter.ErrNoRows) {
		return nil
	}
	require.NoError(t, err)

	return j
}

func TestAdapterQuery(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testAdapterQuery(t, openFunc(t))
		})
	}
}

func testAdapterQuery(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	now := time.Now()
	queue := now.Format(time.RFC3339Nano)

	// schedule several jobs
	j1 := Job{Queue: queue, RunAt: now, Type: "test1"}
	j2 := Job{Queue: queue, RunAt: now, Type: "test2"}
	j3 := Job{Queue: queue, RunAt: now, Type: "test3"}

	err = c.EnqueueBatch(ctx, []*Job{&j1, &j2, &j3})
	require.NoError(t, err)

	// test pool
	testQueryableQuery(ctx, t, connPool, queue, &j1, &j2, &j3)

	// test connection
	conn, err := connPool.Acquire(ctx)
	require.NoError(t, err)

	testQueryableQuery(ctx, t, conn, queue, &j1, &j2, &j3)
	err = conn.Release()
	assert.NoError(t, err)

	// test transaction
	tx, err := connPool.Begin(ctx)
	require.NoError(t, err)

	testQueryableQuery(ctx, t, tx, queue, &j1, &j2, &j3)
	err = tx.Commit(ctx)
	assert.NoError(t, err)
}

func testQueryableQuery(ctx context.Context, t *testing.T, q adapter.Queryable, queue string, j1, j2, j3 *Job) {
	t.Helper()

	rows, err := q.Query(ctx, `SELECT job_id, job_type FROM gue_jobs WHERE queue = $1 ORDER BY job_id ASC`, queue)
	require.NoError(t, err)

	var jobs []*Job
	for rows.Next() {
		j := new(Job)
		err := rows.Scan(&j.ID, &j.Type)
		require.NoError(t, err)

		jobs = append(jobs, j)
	}
	require.Len(t, jobs, 3)

	err = rows.Err()
	require.NoError(t, err)

	assert.Equal(t, j1.ID.String(), jobs[0].ID.String())
	assert.Equal(t, j1.Type, jobs[0].Type)
	assert.Equal(t, j2.ID.String(), jobs[1].ID.String())
	assert.Equal(t, j2.Type, jobs[1].Type)
	assert.Equal(t, j3.ID.String(), jobs[2].ID.String())
	assert.Equal(t, j3.Type, jobs[2].Type)
}

func TestMultiSchema(t *testing.T) {
	connPool := adapterTesting.OpenTestPoolLibPQCustomSchemas(t, "gue", "foo")
	ctx := context.Background()

	// create table with the explicitly set second schema as gue table was already created in the first one
	_, err := connPool.Exec(ctx, "CREATE TABLE IF NOT EXISTS foo.bar ( id serial NOT NULL PRIMARY KEY, data text NOT NULL )")
	require.NoError(t, err)

	// insert into created table w/out setting schema explicitly - search_path should take care of this
	_, err = connPool.Exec(ctx, "INSERT INTO bar (data) VALUES ('baz')")
	require.NoError(t, err)

	// run basic gue client test to ensure it works as expected in its own schema known to search_path
	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{
		Type: "MyJob",
	}
	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.NotEmpty(t, newJob.ID)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})
}

func TestJobIDMigration(t *testing.T) {
	connPool := adapterTesting.OpenTestPoolLibPQCustomSchemas(t, "job_id_migration_01", "job_id_migration_02")
	ctx := context.Background()

	// create a table with the serial ID
	// editorconfig-checker-disable
	_, err := connPool.Exec(ctx, `
DROP TABLE IF EXISTS gue_jobs;
CREATE TABLE gue_jobs
(
  job_id      BIGSERIAL   NOT NULL PRIMARY KEY,
  priority    SMALLINT    NOT NULL,
  run_at      TIMESTAMPTZ NOT NULL,
  job_type    TEXT        NOT NULL,
  args        BYTEA       NOT NULL,
  skip_delete SMALLINT    NOT NULL,
  status      TEXT        NOT NULL,
  error_count INTEGER     NOT NULL DEFAULT 0,
  last_error  TEXT,
  queue       TEXT        NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gue_jobs_selector ON gue_jobs (queue, run_at, priority);
`)
	require.NoError(t, err)
	// editorconfig-checker-enable

	// insert several records to test if the data migration works fine
	now := time.Now()
	const queueName string = "some-queue"
	for i := 0; i < 101; i++ {
		_, err = connPool.Exec(ctx, `INSERT INTO gue_jobs
(queue, priority, run_at, job_type, args, skip_delete, status, created_at, updated_at)
VALUES
($1, $2, $3, $4, $5, $6, $7, $8, $8)
`, queueName, 0, now, "foo-bar", []byte(fmt.Sprintf(`{"job":%d}`, i)), 0, JobStatusTodo, now)
		require.NoError(t, err)
	}

	migrationSQL, err := os.ReadFile("./migrations/job_id_to_ulid.sql")
	require.NoError(t, err)
	_, err = connPool.Exec(ctx, string(migrationSQL))
	require.NoError(t, err)

	// ensure it is possible to retrieve a job from the DB after the conversion
	c, err := NewClient(connPool)
	require.NoError(t, err)

	j1, err := c.LockJob(ctx, queueName)
	require.NoError(t, err)
	require.NotNil(t, j1)
	t.Logf("Locked a job: %s %s", j1.ID.String(), string(j1.Args))

	err = j1.Finish(ctx, JobStatusSuccess)
	require.NoError(t, err)
	err = j1.Done(ctx)
	require.NoError(t, err)

	j2, err := c.LockJobByID(ctx, j1.ID)
	require.Error(t, err)
	require.Nil(t, j2)
}
