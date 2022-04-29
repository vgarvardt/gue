package gue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v4/adapter"
	adapterTesting "github.com/vgarvardt/gue/v4/adapter/testing"
)

func TestEnqueueOnlyType(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueOnlyType(t, openFunc(t))
		})
	}
}

func testEnqueueOnlyType(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	jobType := "MyJob"
	err := c.Enqueue(ctx, &Job{Type: jobType})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

	// check resulting job
	assert.Greater(t, j.ID, int64(0))
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, JobPriorityDefault, j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestEnqueueWithPriority(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithPriority(t, openFunc(t))
		})
	}
}

func testEnqueueWithPriority(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	want := JobPriority(99)
	err := c.Enqueue(ctx, &Job{Type: "MyJob", Priority: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

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
	c := NewClient(connPool)
	ctx := context.Background()

	want := time.Now().Add(2 * time.Minute)
	err := c.Enqueue(ctx, &Job{Type: "MyJob", RunAt: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

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
	c := NewClient(connPool)
	ctx := context.Background()

	want := []byte(`{"arg1":0, "arg2":"a string"}`)
	err := c.Enqueue(ctx, &Job{Type: "MyJob", Args: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

	assert.JSONEq(t, string(want), string(j.Args))
}

func TestEnqueueWithQueue(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testEnqueueWithQueue(t, openFunc(t))
		})
	}
}

func testEnqueueWithQueue(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	want := "special-work-queue"
	err := c.Enqueue(ctx, &Job{Type: "MyJob", Queue: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

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
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: ""})
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
	c := NewClient(connPool)
	ctx := context.Background()

	tx, err := connPool.Begin(ctx)
	require.NoError(t, err)

	err = c.EnqueueTx(ctx, &Job{Type: "MyJob"}, tx)
	require.NoError(t, err)

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	j = findOneJob(t, connPool)
	require.Nil(t, j)
}
