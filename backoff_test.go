package gue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	adapterTesting "github.com/vgarvardt/gue/v5/adapter/testing"
	"go.uber.org/zap/zaptest"

	"github.com/vgarvardt/gue/v5/adapter"
	adapterZap "github.com/vgarvardt/gue/v5/adapter/zap"
)

func TestBackoff(t *testing.T) {
	testBackoff(t, adapterTesting.OpenTestPoolMaxConnsPGXv4(t, 5))
}

func testBackoff(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()
	logger := adapterZap.New(zaptest.NewLogger(t))
	now := time.Now()

	t.Run("default exponential backoff", func(t *testing.T) {
		c, err := NewClient(connPool, WithClientLogger(logger))
		require.NoError(t, err)

		j := Job{RunAt: now, Type: "foo"}
		err = c.Enqueue(ctx, &j)
		require.NoError(t, err)

		jLocked1, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		err = jLocked1.Error(ctx, errors.New("return with the error"))
		require.NoError(t, err)

		jLocked2, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		assert.Equal(t, int32(1), jLocked2.ErrorCount)
		assert.True(t, jLocked2.LastError.Valid)
		assert.Equal(t, "return with the error", jLocked2.LastError.String)
		assert.Greater(t, jLocked2.RunAt.Unix(), jLocked1.RunAt.Unix())

		err = jLocked2.Done(ctx)
		require.NoError(t, err)
	})

	t.Run("never backoff", func(t *testing.T) {
		c, err := NewClient(connPool, WithClientLogger(logger), WithClientBackoff(BackoffNever))
		require.NoError(t, err)

		j := Job{RunAt: now, Type: "bar"}
		err = c.Enqueue(ctx, &j)
		require.NoError(t, err)

		jLocked1, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		err = jLocked1.Error(ctx, errors.New("return with the error"))
		require.NoError(t, err)

		jLocked2, err := c.LockJobByID(ctx, j.ID)
		require.Error(t, err)
		assert.Nil(t, jLocked2)
	})

	t.Run("const backoff", func(t *testing.T) {
		c, err := NewClient(connPool, WithClientLogger(logger), WithClientBackoff(NewConstantBackoff(time.Minute)))
		require.NoError(t, err)

		j := Job{RunAt: now, Type: "foo"}
		err = c.Enqueue(ctx, &j)
		require.NoError(t, err)

		jLocked1, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		err = jLocked1.Error(ctx, errors.New("return with the error"))
		require.NoError(t, err)

		jLocked2, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		assert.Equal(t, int32(1), jLocked2.ErrorCount)
		assert.True(t, jLocked2.LastError.Valid)
		assert.Equal(t, "return with the error", jLocked2.LastError.String)
		assert.Greater(t, jLocked2.RunAt.Unix(), jLocked1.RunAt.Unix())
		assert.WithinDuration(t, jLocked1.RunAt.Add(time.Minute), jLocked2.RunAt, time.Second)

		err = jLocked2.Done(ctx)
		require.NoError(t, err)
	})
}
