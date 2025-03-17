package testing

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv4"
)

// OpenTestPoolMaxConnsPGXv4 opens connections pool used in testing
func OpenTestPoolMaxConnsPGXv4(t testing.TB, maxConnections int32) adapter.ConnPool {
	t.Helper()

	applyMigrations("").Do(func() {
		doApplyMigrations(t, "")
	})

	connPoolConfig, err := pgxpool.ParseConfig(testConnDSN(t))
	require.NoError(t, err)

	connPoolConfig.MaxConns = maxConnections

	poolPGXv4, err := pgxpool.ConnectConfig(context.Background(), connPoolConfig)
	require.NoError(t, err)

	pool := pgxv4.NewConnPool(poolPGXv4)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolPGXv4 opens connections pool used in testing
func OpenTestPoolPGXv4(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsPGXv4(t, defaultPoolConns)
}
