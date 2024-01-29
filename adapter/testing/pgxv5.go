package testing

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv5"
)

// OpenTestPoolMaxConnsPGXv5 opens connections pool used in testing
func OpenTestPoolMaxConnsPGXv5(t testing.TB, maxConnections int32) adapter.ConnPool {
	t.Helper()

	applyMigrations("").Do(func() {
		doApplyMigrations(t, "")
	})

	connPoolConfig, err := pgxpool.ParseConfig(testConnDSN(t))
	require.NoError(t, err)

	connPoolConfig.MaxConns = maxConnections

	poolPGXv5, err := pgxpool.NewWithConfig(context.Background(), connPoolConfig)
	require.NoError(t, err)

	pool := pgxv5.NewConnPool(poolPGXv5)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolPGXv5 opens connections pool used in testing
func OpenTestPoolPGXv5(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsPGXv5(t, defaultPoolConns)
}
