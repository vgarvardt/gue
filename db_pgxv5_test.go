package gue

import (
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// openTestPoolMaxConnsPGXv5 opens connections pool used in testing
func openTestPoolMaxConnsPGXv5(t testing.TB, maxConnections int32) *sql.DB {
	t.Helper()

	applyMigrations("").Do(func() {
		doApplyMigrations(t, "")
	})

	connPoolConfig, err := pgxpool.ParseConfig(testConnDSN(t))
	require.NoError(t, err)

	connPoolConfig.MaxConns = maxConnections

	poolPGXv5, err := pgxpool.NewWithConfig(t.Context(), connPoolConfig)
	require.NoError(t, err)

	db := stdlib.OpenDBFromPool(poolPGXv5)

	t.Cleanup(func() {
		truncateAndClose(t, db)
	})

	return db
}

// openTestPoolPGXv5 opens connections pool used in testing
func openTestPoolPGXv5(t testing.TB) *sql.DB {
	t.Helper()

	return openTestPoolMaxConnsPGXv5(t, defaultPoolConns)
}
