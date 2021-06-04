package gue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v2/adapter"
	adapterTesting "github.com/vgarvardt/gue/v2/adapter/testing"
)

func TestEnqueueOnlyType(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueOnlyType(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueOnlyType(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueOnlyType(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueOnlyType(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
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
	assert.Equal(t, int16(0), j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestEnqueueWithPriority(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueWithPriority(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueWithPriority(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueWithPriority(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueWithPriority(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testEnqueueWithPriority(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	want := int16(99)
	err := c.Enqueue(ctx, &Job{Type: "MyJob", Priority: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

	assert.Equal(t, want, j.Priority)
}

func TestEnqueueWithRunAt(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueWithRunAt(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueWithRunAt(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueWithRunAt(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueWithRunAt(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
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
	want = want.Truncate(time.Microsecond)
	assert.True(t, want.Equal(j.RunAt))
}

func TestEnqueueWithArgs(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueWithArgs(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueWithArgs(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueWithArgs(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueWithArgs(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testEnqueueWithArgs(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	want := []byte(`{"arg1":0, "arg2":"a string"}`)
	err := c.Enqueue(ctx, &Job{Type: "MyJob", Args: want})
	require.NoError(t, err)

	j := findOneJob(t, connPool)
	require.NotNil(t, j)

	assert.Equal(t, want, j.Args)
}

func TestEnqueueWithQueue(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueWithQueue(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueWithQueue(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueWithQueue(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueWithQueue(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
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
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueWithEmptyType(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueWithEmptyType(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueWithEmptyType(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueWithEmptyType(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
}

func testEnqueueWithEmptyType(t *testing.T, connPool adapter.ConnPool) {
	c := NewClient(connPool)
	ctx := context.Background()

	err := c.Enqueue(ctx, &Job{Type: ""})
	require.Equal(t, ErrMissingType, err)
}

func TestEnqueueTx(t *testing.T) {
	t.Run("pgx/v3", func(t *testing.T) {
		testEnqueueTx(t, adapterTesting.OpenTestPoolPGXv3(t))
	})
	t.Run("pgx/v4", func(t *testing.T) {
		testEnqueueTx(t, adapterTesting.OpenTestPoolPGXv4(t))
	})
	t.Run("lib/pq", func(t *testing.T) {
		testEnqueueTx(t, adapterTesting.OpenTestPoolLibPQ(t))
	})
	t.Run("go-pg/v10", func(t *testing.T) {
		testEnqueueTx(t, adapterTesting.OpenTestPoolGoPGv10(t))
	})
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
