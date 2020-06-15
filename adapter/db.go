package adapter

import (
	"context"
	"errors"
)

var (
	// ErrNoRows abstract db driver-level "no rows in result set" error
	ErrNoRows = errors.New("no rows in result set")
	// ErrTxClosed abstract db driver-level "transaction is closed" error
	ErrTxClosed = errors.New("tx is closed")
)

// Row represents single row returned by DB driver
type Row interface {
	// Scan reads the values from the current row into dest values positionally.
	// If no rows were found it returns ErrNoRows. If multiple rows are returned it
	//ignores all but the first.
	Scan(dest ...interface{}) error
}

// CommandTag is the result of an Exec function
type CommandTag interface {
	// RowsAffected returns the number of rows affected. If the CommandTag was not
	// for a row affecting command (such as "CREATE TABLE") then it returns 0
	RowsAffected() int64
}

// Queryable is the base interface for different types of db connections that should implement
// basic querying operations.
type Queryable interface {
	// Exec executes sql. sql can be either a prepared statement name or an SQL string.
	// arguments should be referenced positionally from the sql string as $1, $2, etc.
	Exec(ctx context.Context, sql string, arguments ...interface{}) (CommandTag, error)
	// Query executes sql with args. Any error that occurs while
	// querying is deferred until calling Scan on the returned Row. That Row will
	// error with ErrNoRows if no rows are returned.
	QueryRow(ctx context.Context, sql string, args ...interface{}) Row
}

// Tx represents a database transaction.
type Tx interface {
	Queryable
	// Rollback rolls back the transaction. Rollback will return ErrTxClosed if the
	// Tx is already closed, but is otherwise safe to call multiple times. Hence, a
	// defer tx.Rollback() is safe even if tx.Commit() will be called first in a
	// non-error condition.
	Rollback(ctx context.Context) error
	// Commit commits the transaction
	Commit(ctx context.Context) error
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn interface {
	Queryable
	// Begin starts a transaction with the default transaction mode for the
	// current connection.
	Begin(ctx context.Context) (Tx, error)
	// Close closes a connection. It is safe to call Close on a already closed
	// connection.
	Close(ctx context.Context) error
}

// ConnPoolStat is the connection pool statistics
type ConnPoolStat struct {
	// MaxConnections - max simultaneous connections to use
	MaxConnections int
	// CurrentConnections - current live connections
	CurrentConnections int
	// AvailableConnections - unused live connections
	AvailableConnections int
}

// ConnPool is a PostgreSQL connection pool handle.
type ConnPool interface {
	Queryable
	// Begin starts a transaction with the default transaction mode for the
	// current connection.
	Begin(ctx context.Context) (Tx, error)
	// Acquire takes exclusive use of a connection until it is released.
	Acquire(ctx context.Context) (Conn, error)
	// Release gives up use of a connection.
	Release(conn Conn)
	// Stat returns connection pool statistics
	Stat() ConnPoolStat
	// Close ends the use of a connection pool. It prevents any new connections from
	// being acquired and closes available underlying connections. Any acquired
	// connections will be closed when they are released.
	Close() error
}
