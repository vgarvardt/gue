package gue

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	_ "github.com/lib/pq" // register pq sql driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultPoolConns = 5

var migrations sync.Map

// openTestPool callback type for opening connection pool with default parameters used in tests
type openTestPool func(t testing.TB) *sql.DB

// allAdaptersOpenTestPool lists all available adapters with callbacks
var allAdaptersOpenTestPool = map[string]openTestPool{
	"pgx/v5": openTestPoolPGXv5,
	"lib/pq": openTestPoolLibPQ,
}

func truncateAndClose(t testing.TB, pool *sql.DB) {
	t.Helper()

	_, err := pool.ExecContext(context.WithoutCancel(t.Context()), "TRUNCATE TABLE gue_jobs")
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func applyMigrations(schema string) *sync.Once {
	once, _ := migrations.LoadOrStore(schema, &sync.Once{})
	return once.(*sync.Once)
}

func doApplyMigrations(t testing.TB, schema string) {
	t.Helper()

	dsn := testConnDSN(t)
	if schema != "" {
		dsn += "&search_path=" + schema
		t.Logf("doApplyMigrations dsn: %s", dsn)
	}

	migrationsConn, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	migrationSQL, err := os.ReadFile("./migrations/schema.sql")
	require.NoError(t, err)

	if schema != "" {
		_, err := migrationsConn.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		require.NoError(t, err)
	}

	_, err = migrationsConn.Exec(string(migrationSQL))
	require.NoError(t, err)
}

func testConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	return testPgConnString
	// return `postgres://test:test@localhost:54823/test?sslmode=disable`
}
