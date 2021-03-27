package testing

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v2/adapter"
	"github.com/vgarvardt/gue/v2/adapter/libpq"
)

// OpenTestPoolMaxConnsLibPQ opens connections pool user in testing
func OpenTestPoolMaxConnsLibPQ(t testing.TB, maxConnections int, schema string) adapter.ConnPool {
	t.Helper()

	applyMigrations(schema).Do(func() {
		doApplyMigrations(t, schema)
	})

	db, err := sql.Open("postgres", testConnDSN(t))
	require.NoError(t, err)

	db.SetMaxOpenConns(maxConnections)

	pool := libpq.NewConnPool(db)

	t.Cleanup(func() {
		truncateAndClose(t, pool, schema)
	})

	return pool
}

// OpenTestPoolLibPQ opens connections pool user in testing
func OpenTestPoolLibPQ(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsLibPQ(t, defaultPoolConns, defaultSchema)
}
