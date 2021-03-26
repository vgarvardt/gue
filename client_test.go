package gue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v2/adapter"
	adapterTesting "github.com/vgarvardt/gue/v2/adapter/testing"
)

func TestLockJob(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	defer func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	}()

	// check values of returned Job
	assert.Equal(t, newJob.ID, j.ID)
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, int16(0), j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, newJob.Type, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestLockJobAlreadyLocked(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobAlreadyLocked(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	defer func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	}()
	require.NotNil(t, j)

	j2, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j2)
}

func TestLockJobNoJob(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testLockJobNoJob(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestLockJobCustomQueue(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testLockJobCustomQueue(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	defer func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	}()
	require.NotNil(t, j)

	err = j.Delete(ctx, c.schema)
	require.NoError(t, err)
}

func TestJobTx(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobTx(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobTx(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobTx(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobTx(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	}()

	assert.Equal(t, j.tx, j.Tx())
}

func TestJobConnRace(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobConnRace(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	}()

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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDelete(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
}

func testJobDelete(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	err = j.Delete(ctx, c.schema)
	require.NoError(t, err)

	err = j.Done(ctx)
	require.NoError(t, err)

	// make sure job was deleted
	j2 := findOneJob(t, connPool)
	assert.Nil(t, j2)
}

func TestJobDone(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDone(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobDoneMultiple(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	t.Run("pgx/v3", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	defer func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	}()

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
}

func TestJobErrorCustomBackoff(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobErrorCustomBackoff(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobErrorCustomBackoff(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testJobErrorCustomBackoff(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
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
	defer func() {
		err := j2.Done(ctx)
		assert.NoError(t, err)
	}()

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
	assert.Greater(t, j2.RunAt.Unix(), job.RunAt.Unix())
	assert.Equal(t, job.RunAt.Add(time.Hour).Unix(), j2.RunAt.Unix())
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
		&j.Args,
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
