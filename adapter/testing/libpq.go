package testing

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	"github.com/vgarvardt/gue/adapter/libpq"
)

// OpenTestPoolMaxConnsLibPQ opens connections pool user in testing
func OpenTestPoolMaxConnsLibPQ(t testing.TB, maxConnections int) adapter.ConnPool {
	t.Helper()

	applyMigrations.Do(func() {
		doApplyMigrations(t)
	})

	db, err := sql.Open("postgres", testConnDSN(t))
	require.NoError(t, err)

	db.SetMaxOpenConns(maxConnections)

	pool := libpq.NewConnPool(db)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolLibPQ opens connections pool user in testing
func OpenTestPoolLibPQ(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsLibPQ(t, defaultPoolConns)
}
