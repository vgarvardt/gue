package testing

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql" // register mysql driver
	_ "github.com/lib/pq"              // register pq sql driver
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
	//"pgx/v4": OpenTestPoolPGXv4,
	//"pgx/v5": OpenTestPoolPGXv5,
	//"lib/pq": OpenTestPoolLibPQ,
	"mysql": OpenTestPoolMySQL,
}

func truncateAndClose(t testing.TB, pool adapter.ConnPool) {
	t.Helper()

	_, err := pool.Exec(context.Background(), "TRUNCATE TABLE gue_jobs")
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func applyMigrations(schema string) *sync.Once {
	once, _ := migrations.LoadOrStore(schema, &sync.Once{})
	return once.(*sync.Once)
}

func doApplyPgMigrations(t testing.TB, schema string) {
	t.Helper()

	dsn := testPgConnDSN(t)
	if schema != "" {
		dsn += "&search_path=" + schema
	}

	t.Logf("doApplyPgMigrations dsn: %s", dsn)
	migrationsConn, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	migrationSQL, err := os.ReadFile("./migrations/schema.pg.sql")
	require.NoError(t, err)

	if schema != "" {
		_, err := migrationsConn.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		require.NoError(t, err)
	}

	_, err = migrationsConn.Exec(string(migrationSQL))
	require.NoError(t, err)
}

func doApplyMySQLMigrations(t testing.TB, schema string) {
	t.Helper()

	dsn := testMySQLConnDSN(t)
	if schema != "" {
		dsn += "&search_path=" + schema
	}

	t.Logf("doApplyMySQLMigrations dsn: %s", dsn)
	migrationsConn, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	migrationSQL, err := os.ReadFile("./migrations/schema.mysql.sql")
	require.NoError(t, err)

	if schema != "" {
		_, err := migrationsConn.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		require.NoError(t, err)
	}

	_, err = migrationsConn.Exec(string(migrationSQL))
	require.NoError(t, err)
}

func testPgConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	return testPgConnString
	// return `postgres://test:test@localhost:54823/test?sslmode=disable`
}

func testMySQLConnDSN(t testing.TB) string {
	t.Helper()

	//testMySQLConnString, found := os.LookupEnv("TEST_MYSQL")
	//require.True(t, found, "TEST_MYSQL env var is not set")
	//require.NotEmpty(t, testMySQLConnString, "TEST_MYSQL env var is empty")
	//
	//return strings.Replace(testMySQLConnString, "0.0.0.0", "127.0.0.1", 1)
	return `test:test@tcp(127.0.0.1:58771)/test?charset=utf8&parseTime=true`
}
