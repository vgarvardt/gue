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
	// ignores all but the first.
	Scan(dest ...any) error
}

// CommandTag is the result of an Exec function
type CommandTag interface {
	// RowsAffected returns the number of rows affected. If the CommandTag was not
	// for a row affecting command (such as "CREATE TABLE") then it returns 0
	RowsAffected() int64
}

// Rows represents rows set returned by DB driver
type Rows interface {
	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available. It automatically closes rows
	// when all rows are read.
	Next() bool
	// Scan reads the values from the current row into dest values positionally.
	Scan(dest ...any) error
	// Err returns any error that occurred while reading.
	Err() error
}

// Queryable is the base interface for different types of db connections that should implement
// basic querying operations.
type Queryable interface {
	// Exec executes a query that can be either a prepared statement name or an SQL string.
	// args should be referenced from the query string as question marks (?, ?, etc.)
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	// QueryRow executes query with args. Any error that occurs while
	// querying is deferred until calling Scan on the returned Row. That Row will
	// error with ErrNoRows if no rows are returned.
	QueryRow(ctx context.Context, query string, args ...any) Row
	// Query executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	Query(ctx context.Context, query string, args ...any) (Rows, error)
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

// Conn is a single PostgreSQL connection.
type Conn interface {
	Queryable
	// Ping checks if the DB and connection are alive.
	Ping(ctx context.Context) error
	// Begin starts a transaction with the default transaction mode.
	Begin(ctx context.Context) (Tx, error)
	// Release returns connection to the pool it was acquired from.
	// Once Release has been called, other methods must not be called.
	Release() error
}

// ConnPool is a PostgreSQL connection pool handle.
type ConnPool interface {
	Queryable
	// Ping checks if the DB and connection are alive.
	Ping(ctx context.Context) error
	// Begin starts a transaction with the default transaction mode.
	Begin(ctx context.Context) (Tx, error)
	// Acquire returns a connection Conn from the ConnPool.
	// Connection must be returned to the pool after usage by calling Conn.Release().
	Acquire(ctx context.Context) (Conn, error)
	// Close ends the use of a connection pool. It prevents any new connections from
	// being acquired and closes available underlying connections. Any acquired
	// connections will be closed when they are released.
	Close() error
}
