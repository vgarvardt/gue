package gue

import (
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
)

func testConnDSN(t testing.TB) string {
	testPgConnString := os.Getenv("TEST_POSTGRES")
	if testPgConnString == "" {
		t.Fatal("TEST_PG env var is not set")
	}

	return testPgConnString
}

func testConnConfig(t testing.TB) pgx.ConnConfig {
	cfg, err := pgx.ParseConnectionString(testConnDSN(t))
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

func openTestClientMaxConns(t testing.TB, maxConnections int) *Client {
	migrationsConn, err := sql.Open("pgx", testConnDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := migrationsConn.Close(); err != nil {
			t.Error(err)
		}
	}()

	migrationSQL, err := ioutil.ReadFile("./schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrationsConn.Exec(string(migrationSQL)); err != nil {
		t.Fatal(err)
	}

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnConfig(t),
		MaxConnections: maxConnections,
		AfterConnect:   PrepareStatements,
	}
	pool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		t.Fatal(err)
	}

	return NewClient(pool)
}

func openTestClient(t testing.TB) *Client {
	return openTestClientMaxConns(t, 5)
}

func truncateAndClose(pool *pgx.ConnPool) {
	if _, err := pool.Exec("TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(q queryable) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
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
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}
