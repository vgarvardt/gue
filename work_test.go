package gue

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	adapterTesting "github.com/vgarvardt/gue/adapter/testing"
)

func TestLockJob(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJob(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
}

func testLockJob(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	jobType := "MyJob"
	err := c.Enqueue(ctx, &Job{Type: jobType})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	require.NotNil(t, j.tx)
	require.NotNil(t, j.pool)
	defer j.Done(ctx)

	// check values of returned Job
	assert.Greater(t, j.ID, int64(0))
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, int16(0), j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
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
}

func testLockJobAlreadyLocked(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	j2, err := c.LockJob(ctx, "")
	require.NoError(t, err)

	if j2 != nil {
		defer j2.Done(ctx)
		require.Fail(t, "wanted no job, got %+v", j2)
	}
}

func TestLockJobNoJob(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testLockJobNoJob(t, adapterTesting.OpenTestPoolPGXv4(t))
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
}

func testLockJobCustomQueue(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob", Queue: "extra_priority"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	if j != nil {
		j.Done(ctx)
		assert.Fail(t, "expected no job to be found with empty queue name, got %+v", j)
	}

	j, err = c.LockJob(ctx, "extra_priority")
	require.NoError(t, err)
	defer j.Done(ctx)
	require.NotNil(t, j)

	err = j.Delete(ctx)
	require.NoError(t, err)
}

func TestJobConn(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobTx(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobTx(t, adapterTesting.OpenTestPoolPGXv4(t))
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
	defer j.Done(ctx)

	assert.Equal(t, j.tx, j.Tx())
}

func TestJobConnRace(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobConnRace(t, adapterTesting.OpenTestPoolPGXv4(t))
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
	defer j.Done(ctx)

	var wg sync.WaitGroup
	wg.Add(2)

	// call Tx and Done in different goroutines to make sure they are safe from races
	go func() {
		_ = j.Tx()
		wg.Done()
	}()
	go func() {
		j.Done(ctx)
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
	j.Done(ctx)

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
}

func testJobDone(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	j.Done(ctx)

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
}

func testJobDoneMultiple(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)

	j.Done(ctx)
	// try calling Done() again
	j.Done(ctx)
}

func TestJobError(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testJobError(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
}

func testJobError(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: "MyJob"})
	require.NoError(t, err)

	j, err := c.LockJob(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, j)
	defer j.Done(ctx)

	msg := "world\nended"
	err = j.Error(ctx, msg)
	require.NoError(t, err)

	// make sure job was not deleted
	j2 := findOneJob(t, connPool)
	require.NotNil(t, j2)
	defer j2.Done(ctx)

	assert.NotEqual(t, pgtype.Null, j2.LastError.Status)
	assert.Equal(t, msg, j2.LastError.String)
	assert.Equal(t, int32(1), j2.ErrorCount)
}
