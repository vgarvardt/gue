package testing

import (
	"database/sql"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v5/adapter"
)

const defaultPoolConns = 5

var migrations sync.Map

// OpenTestPool callback type for opening connection pool with default parameters used in tests
type OpenTestPool func(t testing.TB) adapter.ConnPool

// OpenOpenTestPoolMaxConns callback type for opening connection pool with custom max connections used in tests
type OpenOpenTestPoolMaxConns func(t testing.TB, maxConnections int32) adapter.ConnPool

// AllAdaptersOpenTestPool lists all available adapters with callbacks
var AllAdaptersOpenTestPool = map[string]OpenTestPool{
	"pgx/v4": func(t testing.TB) adapter.ConnPool {
		return OpenTestPoolMaxConnsPGXv4(t, 5)
	},
}

func closePool(t testing.TB, pool adapter.ConnPool) {
	t.Helper()

	assert.NoError(t, pool.Close())
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
