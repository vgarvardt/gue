package gue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	adapterTesting "github.com/sadpenguinn/gue/v6/adapter/testing"
)

func TestEnqueueOnlyType(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueOnlyType(t, openFunc(t))
		})
	}
}

func testEnqueueOnlyType(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	jobType := "MyJob"
	job := Job{Type: jobType}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)
	require.False(t, j.CreatedAt.IsZero())

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// check resulting job
	assert.NotEmpty(t, j.ID)
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
	assert.Equal(t, []byte(``), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.False(t, j.LastError.Valid)

	assert.False(t, j.CreatedAt.IsZero())
	assert.True(t, time.Now().After(j.CreatedAt))
	assert.True(
		t,
		job.CreatedAt.Round(time.Second).Equal(j.CreatedAt.Round(time.Second)),
		job.CreatedAt.Round(time.Second).String(),
		j.CreatedAt.Round(time.Second).String(),
	)
}

func TestEnqueueWithPriority(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithPriority(t, openFunc(t))
		})
	}
}

func testEnqueueWithPriority(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	want := JobPriority(99)
	job := Job{Type: "MyJob", Priority: want}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, want, j.Priority)
}

func TestEnqueueWithRunAt(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithRunAt(t, openFunc(t))
		})
	}
}

func testEnqueueWithRunAt(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	want := time.Now().Add(2 * time.Minute)
	job := Job{Type: "MyJob", RunAt: want}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	// truncate to the microsecond as postgres driver does
	assert.WithinDuration(t, want, j.RunAt, time.Microsecond)
}

func TestEnqueueWithArgs(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithArgs(t, openFunc(t))
		})
	}
}

func testEnqueueWithArgs(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	want := []byte(`{"arg1":0, "arg2":"a string"}`)
	job := Job{Type: "MyJob", Args: want}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, want, j.Args)
}

func TestEnqueueWithQueue(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithQueue(t, openFunc(t))
		})
	}
}

func testEnqueueWithQueue(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	want := "special-work-queue"
	job := Job{Type: "MyJob", Queue: want}
	err = c.Enqueue(ctx, &job)
	require.NoError(t, err)

	j, err := c.LockJobByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, j)

	t.Cleanup(func() {
		err := j.Done(ctx)
		assert.NoError(t, err)
	})

	assert.Equal(t, want, j.Queue)
}

func TestEnqueueWithEmptyType(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithEmptyType(t, openFunc(t))
		})
	}
}

func testEnqueueWithEmptyType(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	err = c.Enqueue(ctx, &Job{Type: ""})
	require.Equal(t, ErrMissingType, err)
}

func TestEnqueueTx(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueTx(t, openFunc(t))
		})
	}
}

func testEnqueueTx(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	tx, err := connPool.Begin(ctx)
	require.NoError(t, err)

	job := Job{Type: "MyJob"}
	err = c.EnqueueTx(ctx, &job, tx)
	require.NoError(t, err)

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	j = findOneJob(t, connPool)
	require.Nil(t, j)
}

func TestClient_EnqueueBatchTx(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueBatchTx(t, openFunc(t))
		})
	}
}

func testEnqueueBatchTx(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	tx, err := connPool.Begin(ctx)
	require.NoError(t, err)

	err = c.EnqueueBatchTx(ctx, []*Job{{Type: "MyJob1"}, {Type: "MyJob2"}}, tx)
	require.NoError(t, err)

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	j = findOneJob(t, connPool)
	require.Nil(t, j)
}
