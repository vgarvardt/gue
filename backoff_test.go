package gue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/vgarvardt/gue/v4/adapter"
	adapterTesting "github.com/vgarvardt/gue/v4/adapter/testing"
	"github.com/vgarvardt/gue/v4/adapter/zap"
)

func TestBackoff(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testBackoff(t, openFunc(t))
		})
	}
}

func testBackoff(t *testing.T, connPool adapter.ConnPool) {
	ctx := context.Background()
	logger := zap.New(zaptest.NewLogger(t))
	now := time.Now()

	t.Run("default exponential backoff", func(t *testing.T) {
		c, err := NewClient(connPool, WithClientLogger(logger))
		require.NoError(t, err)

		j := Job{RunAt: now, Type: "foo"}
		err = c.Enqueue(ctx, &j)
		require.NoError(t, err)
		require.NotEmpty(t, j.ID)

		jLocked1, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		err = jLocked1.Error(ctx, "return with the error")
		require.NoError(t, err)

		jLocked2, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		assert.Equal(t, int32(1), jLocked2.ErrorCount)
		assert.NotEqual(t, pgtype.Null, jLocked2.LastError.Status)
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
		require.NotEmpty(t, j.ID)

		jLocked1, err := c.LockJobByID(ctx, j.ID)
		require.NoError(t, err)

		err = jLocked1.Error(ctx, "return with the error")
		require.NoError(t, err)

		jLocked2, err := c.LockJobByID(ctx, j.ID)
		require.Error(t, err)
		assert.Nil(t, jLocked2)
	})
}
