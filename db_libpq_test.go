package gue

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq" // register postgres driver
	"github.com/stretchr/testify/require"
)

// openTestPoolMaxConnsLibPQ opens connections pool used in testing
func openTestPoolMaxConnsLibPQ(t testing.TB, maxConnections int, gueSchema, secondSchema string) *sql.DB {
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

	t.Cleanup(func() {
		truncateAndClose(t, db)
	})

	return db
}

// openTestPoolLibPQ opens connections pool used in testing
func openTestPoolLibPQ(t testing.TB) *sql.DB {
	t.Helper()

	return openTestPoolMaxConnsLibPQ(t, defaultPoolConns, "", "")
}

// openTestPoolLibPQCustomSchemas opens connections pool used in testing with gue table installed to own schema and
// search_path set to two different schemas
func openTestPoolLibPQCustomSchemas(t testing.TB, gueSchema, secondSchema string) *sql.DB {
	t.Helper()

	return openTestPoolMaxConnsLibPQ(t, defaultPoolConns, gueSchema, secondSchema)
}
