package gue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	"github.com/sadpenguinn/gue/v6/adapter/libpq"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv4"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv5"
	adapterTesting "github.com/sadpenguinn/gue/v6/adapter/testing"
)

func TestJob_Tx(t *testing.T) {
	for name, openFunc := range adapterTesting.AllAdaptersOpenTestPool {
		t.Run(name, func(t *testing.T) {
			testJobTxUnwrapTx(t, name, openFunc(t))
		})
	}
}

func testJobTxUnwrapTx(t *testing.T, name string, connPool adapter.ConnPool) {
	ctx := context.Background()

	c, err := NewClient(connPool)
	require.NoError(t, err)

	newJob := &Job{Type: "MyJob", Args: []byte(`{}`)}
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

	switch name {
	case "pgx/v4":
		_, okPgxV5 := pgxv5.UnwrapTx(j.Tx())
		require.False(t, okPgxV5)
		_, okLibPQ := libpq.UnwrapTx(j.Tx())
		require.False(t, okLibPQ)

		tx, okPgxV4 := pgxv4.UnwrapTx(j.Tx())
		require.True(t, okPgxV4)

		_, err := tx.Exec(ctx, `SELECT COUNT(1) FROM gue_jobs`)
		require.NoError(t, err)

	case "pgx/v5":
		_, okPgxV4 := pgxv4.UnwrapTx(j.Tx())
		require.False(t, okPgxV4)
		_, okLibPQ := libpq.UnwrapTx(j.Tx())
		require.False(t, okLibPQ)

		tx, okPgxV5 := pgxv5.UnwrapTx(j.Tx())
		require.True(t, okPgxV5)

		_, err := tx.Exec(ctx, `SELECT COUNT(1) FROM gue_jobs`)
		require.NoError(t, err)

	case "lib/pq":
		_, okPgxV4 := pgxv4.UnwrapTx(j.Tx())
		require.False(t, okPgxV4)
		_, okPgxV5 := pgxv5.UnwrapTx(j.Tx())
		require.False(t, okPgxV5)

		tx, okLibPQ := libpq.UnwrapTx(j.Tx())
		require.True(t, okLibPQ)

		_, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM gue_jobs`)
		require.NoError(t, err)
	}
}
