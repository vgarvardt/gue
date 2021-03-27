package testing

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"text/template"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib" // register pgx sql driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v2/adapter"
	"github.com/vgarvardt/gue/v2/adapter/pgxv3"
)

const defaultPoolConns = 5
const defaultSchema = "public"

var (
	migrations sync.Map
)

func applyMigrations(schema string) *sync.Once {
	once, _ := migrations.LoadOrStore(schema, &sync.Once{})
	return once.(*sync.Once)
}

// OpenTestPoolMaxConnsPGXv3 opens connections pool user in testing
func OpenTestPoolMaxConnsPGXv3(t testing.TB, maxConnections int, schema string) adapter.ConnPool {
	t.Helper()

	applyMigrations(schema).Do(func() {
		doApplyMigrations(t, schema)
	})

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnPGXv3Config(t),
		MaxConnections: maxConnections,
	}
	poolPGXv3, err := pgx.NewConnPool(connPoolConfig)
	require.NoError(t, err)

	pool := pgxv3.NewConnPool(poolPGXv3)

	t.Cleanup(func() {
		truncateAndClose(t, pool, schema)
	})

	return pool
}

// OpenTestPoolPGXv3 opens connections pool user in testing
func OpenTestPoolPGXv3(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsPGXv3(t, defaultPoolConns, defaultSchema)
}

func testConnDSN(t testing.TB) string {
	t.Helper()

	testPgConnString, found := os.LookupEnv("TEST_POSTGRES")
	require.True(t, found, "TEST_POSTGRES env var is not set")
	require.NotEmpty(t, testPgConnString, "TEST_POSTGRES env var is empty")

	//return `postgres://test:test@localhost:32769/test?sslmode=disable`
	return testPgConnString
}

func testConnPGXv3Config(t testing.TB) pgx.ConnConfig {
	t.Helper()

	cfg, err := pgx.ParseConnectionString(testConnDSN(t))
	require.NoError(t, err)

	return cfg
}

func truncateAndClose(t testing.TB, pool adapter.ConnPool, schema string) {
	t.Helper()

	sql := fmt.Sprintf("TRUNCATE TABLE \"%s\".gue_jobs", schema)
	_, err := pool.Exec(context.Background(), sql)
	assert.NoError(t, err)

	err = pool.Close()
	assert.NoError(t, err)
}

func doApplyMigrations(t testing.TB, schema string) {
	t.Helper()

	migrationsConn, err := sql.Open("pgx", testConnDSN(t))
	require.NoError(t, err)
	defer func() {
		err := migrationsConn.Close()
		assert.NoError(t, err)
	}()

	sqlTemplate, err := template.ParseFiles("./schema.sql")
	require.NoError(t, err)

	var buffer bytes.Buffer
	err = sqlTemplate.Execute(&buffer, &TemplateContext{Schema: schema})
	require.NoError(t, err)

	t.Log(buffer.String())
	_, err = migrationsConn.Exec(buffer.String())
	require.NoError(t, err)
}
