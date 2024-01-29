package testing

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"

	"github.com/sadpenguinn/gue/v6/adapter"
	"github.com/sadpenguinn/gue/v6/adapter/libpq"
)

// OpenTestPoolMaxConnsLibPQ opens connections pool used in testing
func OpenTestPoolMaxConnsLibPQ(t testing.TB, maxConnections int, gueSchema, secondSchema string) adapter.ConnPool {
	t.Helper()

	if (gueSchema == "" && secondSchema != "") || (gueSchema != "" && secondSchema == "") {
		require.Fail(t, "Both schemas should be either set or unset")
	}

	applyMigrations(gueSchema).Do(func() {
		doApplyMigrations(t, gueSchema)
	})

	dsn := testConnDSN(t)
	if gueSchema != "" && secondSchema != "" {
		dsn += fmt.Sprintf("&search_path=%s,%s", secondSchema, gueSchema)
	}

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	db.SetMaxOpenConns(maxConnections)

	// guw schema will be created by migrations routine, we need to take care only on the second one
	if secondSchema != "" {
		_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + secondSchema)
		require.NoError(t, err)
	}

	pool := libpq.NewConnPool(db)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolLibPQ opens connections pool used in testing
func OpenTestPoolLibPQ(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsLibPQ(t, defaultPoolConns, "", "")
}

// OpenTestPoolLibPQCustomSchemas opens connections pool used in testing with gue table installed to own schema and
// search_path set to two different schemas
func OpenTestPoolLibPQCustomSchemas(t testing.TB, gueSchema, secondSchema string) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsLibPQ(t, defaultPoolConns, gueSchema, secondSchema)
}
