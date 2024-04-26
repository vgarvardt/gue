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

	// Tt seems that the default isolation level in MySQL is set to "REPEATABLE READ", which means that all queries
	// within the same transaction see the snapshot of the database at the start of the transaction, regardless of
	// the changes made during the transaction.
	// But we need "READ COMMITTED" to allow a transaction to see changes made by other transactions that were
	// committed after the start of the current transaction.
	_, err = db.Exec(`SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`)
	require.NoError(t, err)

	pool := mysql.NewConnPool(db)

	t.Cleanup(func() {
		//truncateAndClose(t, pool)
	})

	return pool
}

// OpenTestPoolMySQL opens connections pool used in testing
func OpenTestPoolMySQL(t testing.TB) adapter.ConnPool {
	t.Helper()

	return OpenTestPoolMaxConnsMySQL(t, defaultPoolConns)
}
