package libpq

import (
	"context"
	"database/sql"

	"github.com/vgarvardt/gue/adapter"
)

// Row implements adapter.Row using github.com/lib/pq
type Row struct {
	row *sql.Row
}

// Scan implements adapter.Row.Scan() using github.com/lib/pq
func (r *Row) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == sql.ErrNoRows {
		return adapter.ErrNoRows
	}

	return err
}

// CommandTag implements adapter.CommandTag using github.com/lib/pq
type CommandTag struct {
	ct sql.Result
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/lib/pq
func (ct CommandTag) RowsAffected() int64 {
	ra, err := ct.ct.RowsAffected()
	if err != nil {
		// TODO: log this error at least
		return 0
	}

	return ra
}

// Tx implements adapter.Tx using github.com/lib/pq
type Tx struct {
	tx *sql.Tx
}

// Exec implements adapter.Tx.Exec() using github.com/lib/pq
func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	query, found := adapter.PreparedStatements[sql]
	if found {
		sql = query
	}

	ct, err := tx.tx.ExecContext(ctx, sql, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/lib/pq
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	query, found := adapter.PreparedStatements[sql]
	if found {
		sql = query
	}

	return &Row{tx.tx.QueryRowContext(ctx, sql, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/lib/pq
func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.tx.Rollback()
	if err == sql.ErrTxDone {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/lib/pq
func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

// Conn implements adapter.Conn using github.com/lib/pq
type Conn struct {
	conn *sql.Conn
}

func newConn(conn *sql.Conn) *Conn {
	return &Conn{conn}
}

// Exec implements adapter.Conn.Exec() using github.com/lib/pq
func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	query, found := adapter.PreparedStatements[sql]
	if found {
		sql = query
	}

	ct, err := c.conn.ExecContext(ctx, sql, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Conn.QueryRow() using github.com/lib/pq
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) adapter.Row {
	query, found := adapter.PreparedStatements[sql]
	if found {
		sql = query
	}

	return &Row{c.conn.QueryRowContext(ctx, sql, args...)}
}

// Begin implements adapter.Conn.Begin() using github.com/lib/pq
func (c *Conn) Begin(ctx context.Context) (adapter.Tx, error) {
	tx, err := c.conn.BeginTx(ctx, nil)
	return &Tx{tx}, err
}

// Release implements adapter.Conn.Release() using github.com/lib/pq
func (c *Conn) Release() {
	err := c.conn.Close()
	if err != nil {
		// TODO: log this error at least
	}
}

// ConnPool implements adapter.ConnPool using github.com/lib/pq
type ConnPool struct {
	pool *sql.DB
}

// NewConnPool instantiates new adapter.ConnPool using github.com/lib/pq
func NewConnPool(pool *sql.DB) adapter.ConnPool {
	return &ConnPool{pool}
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/lib/pq
func (c *ConnPool) Acquire(ctx context.Context) (adapter.Conn, error) {
	conn, err := c.pool.Conn(ctx)
	return newConn(conn), err
}

// Stat implements adapter.ConnPool.Stat() using github.com/lib/pq
func (c *ConnPool) Stat() adapter.ConnPoolStat {
	s := c.pool.Stats()
	return adapter.ConnPoolStat{
		MaxConnections:       s.MaxOpenConnections,
		CurrentConnections:   s.InUse,
		AvailableConnections: s.Idle,
	}
}

// Close implements adapter.ConnPool.Close() using github.com/lib/pq
func (c *ConnPool) Close() error {
	return c.pool.Close()
}
