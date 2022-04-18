package testing

import (
	"context"
	"testing"

	"github.com/go-pg/pg/v10"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v4/adapter"
	"github.com/vgarvardt/gue/v4/adapter/gopgv10"
)

// OpenTestPoolMaxConnsGoPGv10 opens connections pool used in testing
func OpenTestPoolMaxConnsGoPGv10(t testing.TB, maxConnections int32) adapter.ConnPool {
	t.Helper()

	applyMigrations("").Do(func() {
		doApplyMigrations(t, "")
	})

	opts, err := pg.ParseURL(testConnDSN(t))
	require.NoError(t, err)

	opts.PoolSize = int(maxConnections)

	db := pg.Connect(opts)
	require.NoError(t, db.Ping(context.Background()))

	pool := gopgv10.NewConnPool(db)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolGoPGv10 opens connections pool used in testing
func OpenTestPoolGoPGv10(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsGoPGv10(t, defaultPoolConns)
}
