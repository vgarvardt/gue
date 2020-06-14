package pgXv3

import (
	"github.com/jackc/pgx"

	"github.com/vgarvardt/gue/adapter"
)

type Row struct {
	row *pgx.Row
}

func (r *Row) Scan(dest ...interface{}) error {
	err := r.row.Scan(dest...)
	if err == pgx.ErrNoRows {
		return adapter.ErrNoRows
	}

	return err
}

type CommandTag struct {
	ct pgx.CommandTag
}

func (ct CommandTag) RowsAffected() int64 {
	return ct.ct.RowsAffected()
}

type Tx struct {
	tx *pgx.Tx
}

func (tx *Tx) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := tx.tx.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

func (tx *Tx) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{tx.tx.QueryRow(sql, args...)}
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

type Conn struct {
	conn *pgx.Conn
}

func NewConn(conn *pgx.Conn) adapter.Conn {
	return &Conn{conn}
}

func (c *Conn) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.conn.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

func (c *Conn) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{c.conn.QueryRow(sql, args...)}
}

func (c *Conn) Begin() (adapter.Tx, error) {
	tx, err := c.conn.Begin()
	return &Tx{tx}, err
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

type ConnPool struct {
	pool *pgx.ConnPool
}

func NewConnPool(pool *pgx.ConnPool) adapter.ConnPool {
	return &ConnPool{pool}
}

func (c *ConnPool) Exec(sql string, arguments ...interface{}) (adapter.CommandTag, error) {
	ct, err := c.pool.Exec(sql, arguments...)
	return CommandTag{ct}, err
}

func (c *ConnPool) QueryRow(sql string, args ...interface{}) adapter.Row {
	return &Row{c.pool.QueryRow(sql, args...)}
}

func (c *ConnPool) Begin() (adapter.Tx, error) {
	tx, err := c.pool.Begin()
	return &Tx{tx}, err
}

func (c *ConnPool) Acquire() (adapter.Conn, error) {
	conn, err := c.pool.Acquire()
	return &Conn{conn}, err
}

func (c *ConnPool) Release(conn adapter.Conn) {
	c.pool.Release(conn.(*Conn).conn)
}

func (c *ConnPool) Stat() adapter.ConnPoolStat {
	s := c.pool.Stat()
	return adapter.ConnPoolStat{
		MaxConnections:       s.MaxConnections,
		CurrentConnections:   s.CurrentConnections,
		AvailableConnections: s.AvailableConnections,
	}
}

func (c *ConnPool) Close() {
	c.pool.Close()
}
