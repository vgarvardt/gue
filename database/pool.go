package database

import "github.com/jackc/pgx/v4/pgxpool"

func (q *Queries) Pool() *pgxpool.Pool {
	return q.db.(*pgxpool.Pool)
}
