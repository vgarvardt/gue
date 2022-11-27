package testing

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql" // register mysql driver
	"github.com/stretchr/testify/require"

	"github.com/vgarvardt/gue/v5/adapter"
	"github.com/vgarvardt/gue/v5/adapter/mysql"
)

// OpenTestPoolMaxConnsMySQL opens connections pool used in testing
func OpenTestPoolMaxConnsMySQL(t testing.TB, maxConnections int) adapter.ConnPool {
	t.Helper()

	applyMigrations("mysql/").Do(func() {
		doApplyMySQLMigrations(t, "")
	})

	dsn := testMySQLConnDSN(t)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)

	db.SetMaxOpenConns(maxConnections)

	pool := mysql.NewConnPool(db)

	t.Cleanup(func() {
		truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolMySQL opens connections pool used in testing
func OpenTestPoolMySQL(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsMySQL(t, defaultPoolConns)
}
