package testing

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/sadpenguinn/gue/v6/adapter"
)

var (
	_ adapter.Row        = &Row{}
	_ adapter.Rows       = &Rows{}
	_ adapter.CommandTag = &CommandTag{}
	_ adapter.Queryable  = &Queryable{}
	_ adapter.Tx         = &Tx{}
	_ adapter.Conn       = &Conn{}
	_ adapter.ConnPool   = &ConnPool{}
)

// Row mock implementation of adapter.Row
type Row struct {
	mock.Mock
}

// Scan mock implementation of adapter.Row.Scan()
func (m *Row) Scan(dest ...any) error {
	args := m.Called(dest...)
	return args.Error(0)
}

// CommandTag mock implementation of adapter.CommandTag
type CommandTag struct {
	mock.Mock
}

// RowsAffected mock implementation of adapter.CommandTag.RowsAffected()
func (m *CommandTag) RowsAffected() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

// Rows mock implementation of adapter.Rows
type Rows struct {
	mock.Mock
}

// Next mock implementation of adapter.Rows.Next()
func (m *Rows) Next() bool {
	args := m.Called()
	return args.Bool(0)
}

// Scan mock implementation of adapter.Rows.Scan()
func (m *Rows) Scan(dest ...any) error {
	args := m.Called(dest...)
	return args.Error(0)
}

// Err mock implementation of adapter.Rows.Err()
func (m *Rows) Err() error {
	args := m.Called()
	return args.Error(0)
}

// Queryable mock implementation of adapter.Queryable
type Queryable struct {
	mock.Mock
}

// Exec mock implementation of adapter.Queryable.Exec()
func (m *Queryable) Exec(ctx context.Context, query string, args ...any) (adapter.CommandTag, error) {
	mArgs := m.Called(ctx, query, args)
	arg0 := mArgs.Get(0)
	if arg0 == nil {
		return nil, mArgs.Error(1)
	}
	return arg0.(adapter.CommandTag), mArgs.Error(1)
}

// QueryRow mock implementation of adapter.Queryable.QueryRow()
func (m *Queryable) QueryRow(ctx context.Context, query string, args ...any) adapter.Row {
	mArgs := m.Called(ctx, query, args)
	return mArgs.Get(0).(adapter.Row)
}

// Query mock implementation of adapter.Queryable.Query()
func (m *Queryable) Query(ctx context.Context, query string, args ...any) (adapter.Rows, error) {
	mArgs := m.Called(ctx, query, args)
	arg0 := mArgs.Get(0)
	if arg0 == nil {
		return nil, mArgs.Error(1)
	}
	return arg0.(adapter.Rows), mArgs.Error(1)
}

// Tx mock implementation of adapter.Tx
type Tx struct {
	Queryable
	mock.Mock
}

// Rollback mock implementation of adapter.Tx.Rollback()
func (m *Tx) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Commit mock implementation of adapter.Tx.Commit()
func (m *Tx) Commit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Conn mock implementation of adapter.Conn
type Conn struct {
	Queryable
	mock.Mock
}

// Ping mock implementation of adapter.Conn.Ping()
func (m *Conn) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Begin mock implementation of adapter.Conn.Begin()
func (m *Conn) Begin(ctx context.Context) (adapter.Tx, error) {
	mArgs := m.Called(ctx)
	arg0 := mArgs.Get(0)
	if arg0 == nil {
		return nil, mArgs.Error(1)
	}
	return arg0.(adapter.Tx), mArgs.Error(1)
}

// Release mock implementation of adapter.Conn.Release()
func (m *Conn) Release() error {
	args := m.Called()
	return args.Error(0)
}

// ConnPool mock implementation of adapter.ConnPool
type ConnPool struct {
	Queryable
	mock.Mock
}

// Ping mock implementation of adapter.ConnPool.Ping()
func (m *ConnPool) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Begin mock implementation of adapter.ConnPool.Begin()
func (m *ConnPool) Begin(ctx context.Context) (adapter.Tx, error) {
	mArgs := m.Called(ctx)
	arg0 := mArgs.Get(0)
	if arg0 == nil {
		return nil, mArgs.Error(1)
	}
	return arg0.(adapter.Tx), mArgs.Error(1)
}

// Acquire mock implementation of adapter.ConnPool.Acquire()
func (m *ConnPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	mArgs := m.Called(ctx)
	arg0 := mArgs.Get(0)
	if arg0 == nil {
		return nil, mArgs.Error(1)
	}
	return arg0.(adapter.Conn), mArgs.Error(1)
}

// Close mock implementation of adapter.ConnPool.Close()
func (m *ConnPool) Close() error {
	args := m.Called()
	return args.Error(0)
}
