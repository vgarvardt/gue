package testing

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	"github.com/vgarvardt/gue/adapter/pgxv4"
)

// OpenTestPoolMaxConnsPGXv4 opens connections pool user in testing
func OpenTestPoolMaxConnsPGXv4(t testing.TB, maxConnections int32) adapter.ConnPool {
	t.Helper()

	applyMigrations.Do(func() {
		doApplyMigrations(t)
	})

	connPoolConfig, err := pgxpool.ParseConfig(testConnDSN(t))
	require.NoError(t, err)

	connPoolConfig.MaxConns = maxConnections
	connPoolConfig.AfterConnect = pgxv4.PrepareStatements

	poolPGXv4, err := pgxpool.ConnectConfig(context.Background(), connPoolConfig)
	require.NoError(t, err)

	pool := pgxv4.NewConnPool(poolPGXv4)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolPGXv4 opens connections pool user in testing
func OpenTestPoolPGXv4(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsPGXv4(t, defaultPoolConns)
}

// OpenTestConnPGXv4 opens connection user in testing
func OpenTestConnPGXv4(t testing.TB) adapter.Conn {
	t.Helper()

	conn, err := pgx.ConnectConfig(context.Background(), testConnPGXv4Config(t))
	require.NoError(t, err)

	return pgxv4.NewConn(conn)
}

func testConnPGXv4Config(t testing.TB) *pgx.ConnConfig {
	t.Helper()

	cfg, err := pgx.ParseConfig(testConnDSN(t))
	require.NoError(t, err)

	return cfg
}
