package testing

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib" // register pgx sql driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v2/adapter"
	"github.com/vgarvardt/gue/v2/adapter/pgxv3"
)

const defaultPoolConns = 5

var (
	migrations sync.Map
)

func applyMigrations(schema string) *sync.Once {
	once, _ := migrations.LoadOrStore(schema, &sync.Once{})
	return once.(*sync.Once)
}

// OpenTestPoolMaxConnsPGXv3 opens connections pool used in testing
func OpenTestPoolMaxConnsPGXv3(t testing.TB, maxConnections int) adapter.ConnPool {
	t.Helper()

	applyMigrations("").Do(func() {
		doApplyMigrations(t, "")
	})

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnPGXv3Config(t),
		MaxConnections: maxConnections,
	}
	poolPGXv3, err := pgx.NewConnPool(connPoolConfig)
	require.NoError(t, err)

	pool := pgxv3.NewConnPool(poolPGXv3)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolPGXv3 opens connections pool used in testing
func OpenTestPoolPGXv3(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsPGXv3(t, defaultPoolConns)
}

func testConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	return testPgConnString
	//return `postgres://test:test@localhost:54823/test?sslmode=disable`
}

func testConnPGXv3Config(t testing.TB) pgx.ConnConfig {
	t.Helper()

	cfg, err := pgx.ParseConnectionString(testConnDSN(t))
	require.NoError(t, err)

	return cfg
}

func truncateAndClose(t testing.TB, pool adapter.ConnPool) {
	t.Helper()

	_, err := pool.Exec(context.Background(), "TRUNCATE TABLE gue_jobs")
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func doApplyMigrations(t testing.TB, schema string) {
	t.Helper()

	dsn := testConnDSN(t)
	if schema != "" {
		dsn += "&search_path=" + schema
		t.Logf("doApplyMigrations dsn: %s", dsn)
	}

	migrationsConn, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	migrationSQL, err := ioutil.ReadFile("./schema.sql")
	require.NoError(t, err)

	if schema != "" {
		_, err := migrationsConn.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		require.NoError(t, err)
	}

	_, err = migrationsConn.Exec(string(migrationSQL))
	require.NoError(t, err)
}
