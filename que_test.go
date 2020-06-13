package gue

import (
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	return testPgConnString
}

func testConnConfig(t testing.TB) pgx.ConnConfig {
	t.Helper()

	cfg, err := pgx.ParseConnectionString(testConnDSN(t))
	require.NoError(t, err)

	return cfg
}

func openTestClientMaxConns(t testing.TB, maxConnections int) *Client {
	t.Helper()

	migrationsConn, err := sql.Open("pgx", testConnDSN(t))
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	migrationSQL, err := ioutil.ReadFile("./schema.sql")
	require.NoError(t, err)

	_, err = migrationsConn.Exec(string(migrationSQL))
	require.NoError(t, err)

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnConfig(t),
		MaxConnections: maxConnections,
		AfterConnect:   PrepareStatements,
	}
	pool, err := pgx.NewConnPool(connPoolConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return NewClient(pool)
}

func openTestClient(t testing.TB) *Client {
	t.Helper()

	return openTestClientMaxConns(t, 5)
}

func truncateAndClose(t testing.TB, pool *pgx.ConnPool) {
	t.Helper()

	_, err := pool.Exec("TRUNCATE TABLE que_jobs")
	assert.NoError(t, err)

	pool.Close()
}

func findOneJob(t testing.TB, q queryable) *Job {
	t.Helper()

	findSQL := `SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue FROM que_jobs LIMIT 1`

	j := new(Job)
	err := q.QueryRow(findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == pgx.ErrNoRows {
		return nil
	}
	require.NoError(t, err)

	return j
}
