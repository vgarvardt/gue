package adapter

import (
	"errors"
)

var ErrNoRows = errors.New("no rows in result set")

type Row interface {
	Scan(dest ...interface{}) error
}

type CommandTag interface {
	RowsAffected() int64
}

type Queryable interface {
	Exec(sql string, arguments ...interface{}) (CommandTag, error)
	QueryRow(sql string, args ...interface{}) Row
}

type Tx interface {
	Queryable
	Rollback() error
	Commit() error
}

type Conn interface {
	Queryable
	Begin() (Tx, error)
	Close() error
}

type ConnPoolStat struct {
	MaxConnections       int // max simultaneous connections to use
	CurrentConnections   int // current live connections
	AvailableConnections int // unused live connections
}

type ConnPool interface {
	Queryable
	Begin() (Tx, error)
	Acquire() (Conn, error)
	Release(conn Conn)
	Stat() ConnPoolStat
	Close()
}
