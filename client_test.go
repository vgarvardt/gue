package gue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v4/adapter"
	adapterTesting "github.com/vgarvardt/gue/v4/adapter/testing"
)

func TestLockJob(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJob(t, openFunc(t))
		})
	}
}

func testLockJob(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	newJob := &Job{
		Type: "MyJob",
	}
	err := c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.Greater(t, newJob.ID, int64(0))

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)
	require.NotNil(t, j.pool)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID, j.ID)
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestLockJobAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	c := NewClient(connPool)
	ctx := context.Background()

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
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob", Queue: "extra_priority"})
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
	c := NewClient(connPool)
	ctx := context.Background()

	newJob := &Job{
		Type: "MyJob",
	}
	err := c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.Greater(t, newJob.ID, int64(0))

	j, err := c.LockJobByID(ctx, newJob.ID)
	require.NoError(t, err)

	require.NotNil(t, j.tx)
	require.NotNil(t, j.pool)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID, j.ID)
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestLockJobByIDAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockJobByIDAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockJobByIDAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	newJob := &Job{
		Type: "MyJob",
	}

	err := c.Enqueue(ctx, newJob)
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
	c := NewClient(connPool)
	ctx := context.Background()

	j, err := c.LockJobByID(ctx, 0)
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
	c := NewClient(connPool)
	ctx := context.Background()

	newJob := &Job{
		Type:  "MyJob",
		RunAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	err := c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.Greater(t, newJob.ID, int64(0))

	j, err := c.LockNextScheduledJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)
	require.NotNil(t, j.pool)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check values of returned Job
	assert.Equal(t, newJob.ID, j.ID)
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestLockNextScheduledJobAlreadyLocked(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testLockNextScheduledJobAlreadyLocked(t, openFunc(t))
		})
	}
}

func testLockNextScheduledJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	c := NewClient(connPool)
	ctx := context.Background()

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
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Delete(ctx)
	require.NoError(t, err)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure job was deleted
	j2 := findOneJob(t, connPool)
	assert.Nil(t, j2)
}

func TestJobDone(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobDone(t, openFunc(t))
		})
	}
}

func testJobDone(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure conn and pool were cleared
	assert.Nil(t, j.tx)
	assert.Nil(t, j.pool)
}

func TestJobDoneMultiple(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobDoneMultiple(t, openFunc(t))
		})
	}
}

func testJobDoneMultiple(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
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
	c := NewClient(connPool)
	ctx := context.Background()

	job := &Job{Type: "MyJob"}
	err := c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, msg)
	require.NoError(t, err)

	// make sure job was not deleted
	j2 := findOneJob(t, connPool)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
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
	customBackoff := func(retries int) time.Duration {
		return time.Duration(retries) * time.Hour
	}

	c := NewClient(connPool, WithClientBackoff(customBackoff))
	ctx := context.Background()

	job := &Job{Type: "MyJob"}
	err := c.Enqueue(ctx, job)
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	msg := "world\nended"
	err = j.Error(ctx, msg)
	require.NoError(t, err)

	// make sure job was not deleted
	j2 := findOneJob(t, connPool)
	require.NotNil(t, j2)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
	assert.Equal(t, job.RunAt.Add(time.Hour).Unix(), j2.RunAt.Unix())
}

func TestJobPriority(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobPriority(t, openFunc(t))
		})
	}
}

func testJobPriority(t *testing.T, connPool adapter.ConnPool) {
	var err error

	c := NewClient(connPool)
	ctx := context.Background()

	// insert in the order different from expected to be locked
	jobPriorityDefault := &Job{Type: "MyJob", Priority: JobPriorityDefault, Args: []byte(`"default"`)}
	err = c.Enqueue(ctx, jobPriorityDefault)
	require.NoError(t, err)

	jobPriorityLowest := &Job{Type: "MyJob", Priority: JobPriorityLowest, Args: []byte(`"lowest"`)}
	err = c.Enqueue(ctx, jobPriorityLowest)
	require.NoError(t, err)

	jobPriorityHighest := &Job{Type: "MyJob", Priority: JobPriorityHighest, Args: []byte(`"highest"`)}
	err = c.Enqueue(ctx, jobPriorityHighest)
	require.NoError(t, err)

	jobPriorityLow := &Job{Type: "MyJob", Priority: JobPriorityLow, Args: []byte(`"low"`)}
	err = c.Enqueue(ctx, jobPriorityLow)
	require.NoError(t, err)

	jobPriorityHigh := &Job{Type: "MyJob", Priority: JobPriorityHigh, Args: []byte(`"high"`)}
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
		`SELECT priority, run_at, job_id, job_type, args, error_count, last_error, queue FROM gue_jobs LIMIT 1`,
	).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		(*json.RawMessage)(&j.Args),
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == adapter.ErrNoRows {
		return nil
	}
	require.NoError(t, err)

	return j
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
	c := NewClient(connPool)

	newJob := &Job{
		Type: "MyJob",
	}
	err = c.Enqueue(ctx, newJob)
	require.NoError(t, err)
	require.Greater(t, newJob.ID, int64(0))

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)
	require.NotNil(t, j.pool)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})
}
