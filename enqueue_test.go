package gue

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueueOnlyType(t *testing.T) {
	c := openTestClient(t)

	jobType := "MyJob"
	err := c.Enqueue(&Job{Type: jobType})
	require.NoError(t, err)

	j := findOneJob(t, c.pool)
	require.NotNil(t, j)

	// check resulting job
	assert.Greater(t, j.ID, int64(0))
	assert.Equal(t, defaultQueueName, j.Queue)
	assert.Equal(t, int16(100), j.Priority)
	assert.False(t, j.RunAt.IsZero())
	assert.Equal(t, jobType, j.Type)
	assert.Equal(t, []byte(`[]`), j.Args)
	assert.Equal(t, int32(0), j.ErrorCount)
	assert.NotEqual(t, pgtype.Present, j.LastError.Status)
}

func TestEnqueueWithPriority(t *testing.T) {
	c := openTestClient(t)

	want := int16(99)
	err := c.Enqueue(&Job{Type: "MyJob", Priority: want})
	require.NoError(t, err)

	j := findOneJob(t, c.pool)
	require.NotNil(t, j)

	assert.Equal(t, want, j.Priority)
}

func TestEnqueueWithRunAt(t *testing.T) {
	c := openTestClient(t)

	want := time.Now().Add(2 * time.Minute)
	err := c.Enqueue(&Job{Type: "MyJob", RunAt: want})
	require.NoError(t, err)

	j := findOneJob(t, c.pool)
	require.NotNil(t, j)

	// truncate to the microsecond as postgres driver does
	want = want.Truncate(time.Microsecond)
	assert.True(t, want.Equal(j.RunAt))
}

func TestEnqueueWithArgs(t *testing.T) {
	c := openTestClient(t)

	want := []byte(`{"arg1":0, "arg2":"a string"}`)
	err := c.Enqueue(&Job{Type: "MyJob", Args: want})
	require.NoError(t, err)

	j := findOneJob(t, c.pool)
	require.NotNil(t, j)

	assert.Equal(t, want, j.Args)
}

func TestEnqueueWithQueue(t *testing.T) {
	c := openTestClient(t)

	want := "special-work-queue"
	err := c.Enqueue(&Job{Type: "MyJob", Queue: want})
	require.NoError(t, err)

	j := findOneJob(t, c.pool)
	require.NotNil(t, j)

	assert.Equal(t, want, j.Queue)
}

func TestEnqueueWithEmptyType(t *testing.T) {
	c := openTestClient(t)

	err := c.Enqueue(&Job{Type: ""})
	require.Equal(t, ErrMissingType, err)
}

func TestEnqueueInTx(t *testing.T) {
	c := openTestClient(t)

	tx, err := c.pool.Begin()
	require.NoError(t, err)

	err = c.EnqueueInTx(&Job{Type: "MyJob"}, tx)
	require.NoError(t, err)

	j := findOneJob(t, tx)
	require.NotNil(t, j)

	err = tx.Rollback()
	require.NoError(t, err)

	j = findOneJob(t, c.pool)
	require.Nil(t, j)
}
