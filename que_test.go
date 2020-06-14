package gue

import (
	"database/sql"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/adapter"
	"github.com/vgarvardt/gue/adapter/pgxv3"
)

var (
	applyMigrations sync.Once
)

func testConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	return testPgConnString
}

func testConnPGXv3Config(t testing.TB) pgx.ConnConfig {
	t.Helper()

	cfg, err := pgx.ParseConnectionString(testConnDSN(t))
	require.NoError(t, err)

	return cfg
}

func openTestClientMaxConnsPGXv3(t testing.TB, maxConnections int) *Client {
	t.Helper()

	applyMigrations.Do(func() {
		doApplyMigrations(t)
	})

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnPGXv3Config(t),
		MaxConnections: maxConnections,
		AfterConnect:   pgxv3.PrepareStatements,
	}
	poolPGXv3, err := pgx.NewConnPool(connPoolConfig)
	require.NoError(t, err)

	pool := pgxv3.NewConnPool(poolPGXv3)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return NewClient(pool)
}

func openTestClientPGXv3(t testing.TB) *Client {
	t.Helper()

	return openTestClientMaxConnsPGXv3(t, 5)
}

func openTestConnPGXv3(t testing.TB) adapter.Conn {
	t.Helper()

	conn, err := pgx.Connect(testConnPGXv3Config(t))
	require.NoError(t, err)

	return pgxv3.NewConn(conn)
}

func truncateAndClose(t testing.TB, pool adapter.ConnPool) {
	t.Helper()

	_, err := pool.Exec("TRUNCATE TABLE que_jobs")
	assert.NoError(t, err)

	pool.Close()
}

func findOneJob(t testing.TB, q adapter.Queryable) *Job {
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
	if err == adapter.ErrNoRows {
		return nil
	}
	require.NoError(t, err)

	return j
}

func doApplyMigrations(t testing.TB) {
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
}
