package gue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/vgarvardt/gue/v5/adapter"
	adapterTesting "github.com/vgarvardt/gue/v5/adapter/testing"
	adapterZap "github.com/vgarvardt/gue/v5/adapter/zap"
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

	err = j.Delete(ctx)
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

func TestJobDelete(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobDelete(t, openFunc(t))
		})
	}
}

func testJobDelete(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Delete(ctx)
	require.NoError(t, err)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure job was deleted
	jj, err := c.LockJobByID(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrNoRows))
	assert.Nil(t, jj)
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

	logger := adapterZap.New(zaptest.NewLogger(t))
	c, err := NewClient(connPool, WithClientLogger(logger))
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
		logger.Debug("Locking job", adapter.F("priority", expected.Priority))

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

	rows, err := q.Query(ctx, `SELECT job_id, job_type FROM gue_jobs WHERE queue = ? ORDER BY job_id ASC`, queue)
	require.NoError(t, err)

	var jobs []*Job
	for rows.Next() {
		// TODO: find out better way of reading ULID in MySQL, currently it is being read as []byte and Scan
		// is trying to unmarshal from binary format, although it is actually a string representation
		var jID []byte

		j := new(Job)
		err := rows.Scan(&jID, &j.Type)
		require.NoError(t, err)

		err = (&j.ID).UnmarshalText(jID)
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
