package pgxv3

import (
	"github.com/jackc/pgx"

	"github.com/vgarvardt/gue/adapter"
)

// Row implements adapter.Row using github.com/jackc/pgx/v3
type Row struct {
	row *pgx.Row
}

// Scan implements adapter.Row.Scan() using github.com/jackc/pgx/v3
func (r *Row) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == pgx.ErrNoRows {
		return adapter.ErrNoRows
	}

	return err
}

// CommandTag implements adapter.CommandTag using github.com/jackc/pgx/v3
type CommandTag struct {
	ct pgx.CommandTag
}

// RowsAffected implements adapter.CommandTag.RowsAffected() using github.com/jackc/pgx/v3
func (ct CommandTag) RowsAffected() int64 {
	return ct.ct.RowsAffected()
}

// Tx implements adapter.Tx using github.com/jackc/pgx/v3
type Tx struct {
	tx *pgx.Tx
}

// Exec implements adapter.Tx.Exec() using github.com/jackc/pgx/v3
func (tx *Tx) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := tx.tx.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Tx.QueryRow() using github.com/jackc/pgx/v3
func (tx *Tx) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{tx.tx.QueryRow(sql, args...)}
}

// Rollback implements adapter.Tx.Rollback() using github.com/jackc/pgx/v3
func (tx *Tx) Rollback() error {
	err := tx.tx.Rollback()
	if err == pgx.ErrTxClosed {
		return adapter.ErrTxClosed
	}

	return err
}

// Commit implements adapter.Tx.Commit() using github.com/jackc/pgx/v3
func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

// Conn implements adapter.Conn using github.com/jackc/pgx/v3
type Conn struct {
	conn *pgx.Conn
}

// NewConn instantiates new adapter.Conn using github.com/jackc/pgx/v3
func NewConn(conn *pgx.Conn) adapter.Conn {
	return &Conn{conn}
}

// Exec implements adapter.Conn.Exec() using github.com/jackc/pgx/v3
func (c *Conn) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.conn.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.Conn.QueryRow() using github.com/jackc/pgx/v3
func (c *Conn) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{c.conn.QueryRow(sql, args...)}
}

// Begin implements adapter.Conn.Begin() using github.com/jackc/pgx/v3
func (c *Conn) Begin() (adapter.Tx, error) {
	tx, err := c.conn.Begin()
	return &Tx{tx}, err
}

// Close implements adapter.Conn.Close() using github.com/jackc/pgx/v3
func (c *Conn) Close() error {
	return c.conn.Close()
}

// ConnPool implements adapter.ConnPool using github.com/jackc/pgx/v3
type ConnPool struct {
	pool *pgx.ConnPool
}

// NewConnPool instantiates new adapter.ConnPool using github.com/jackc/pgx/v3
func NewConnPool(pool *pgx.ConnPool) adapter.ConnPool {
	return &ConnPool{pool}
}

// Exec implements adapter.ConnPool.Exec() using github.com/jackc/pgx/v3
func (c *ConnPool) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.pool.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

// QueryRow implements adapter.ConnPool.QueryRow() using github.com/jackc/pgx/v3
func (c *ConnPool) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{c.pool.QueryRow(sql, args...)}
}

// Begin implements adapter.ConnPool.Begin() using github.com/jackc/pgx/v3
func (c *ConnPool) Begin() (adapter.Tx, error) {
	tx, err := c.pool.Begin()
	return &Tx{tx}, err
}

// Acquire implements adapter.ConnPool.Acquire() using github.com/jackc/pgx/v3
func (c *ConnPool) Acquire() (adapter.Conn, error) {
	conn, err := c.pool.Acquire()
	return &Conn{conn}, err
}

// Release implements adapter.ConnPool.Release() using github.com/jackc/pgx/v3
func (c *ConnPool) Release(conn adapter.Conn) {
	c.pool.Release(conn.(*Conn).conn)
}

// Stat implements adapter.ConnPool.Stat() using github.com/jackc/pgx/v3
func (c *ConnPool) Stat() adapter.ConnPoolStat {
	s := c.pool.Stat()
	return adapter.ConnPoolStat{
		MaxConnections:       s.MaxConnections,
		CurrentConnections:   s.CurrentConnections,
		AvailableConnections: s.AvailableConnections,
	}
}

// Close implements adapter.ConnPool.Close() using github.com/jackc/pgx/v3
func (c *ConnPool) Close() {
	c.pool.Close()
}
