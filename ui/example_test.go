package asynqmon_test

import (
	"context"
	"log"
	"net/http"
	"testing"

	"github.com/2tvenom/gue/ui"
	"github.com/2tvenom/gue/ui/database"
	"github.com/jackc/pgx/v4/pgxpool"
)

func Test_HTTPHandler(t *testing.T) {
	connPoolConfig, err := pgxpool.ParseConfig("postgres://processing:Ua2AtuBBZKBcTEg@127.0.0.1:6432/test?sslmode=disable&prefer_simple_protocol=true")
	if err != nil {
		log.Fatal(err)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), connPoolConfig)
	if err != nil {
		log.Fatal(err)
	}

	h := asynqmon.New(asynqmon.Options{
		RootPath: "/queue",
		ConnOpt:  database.New(pool),
	})

	http.Handle("/", h)
	log.Fatal(http.ListenAndServe(":8000", nil)) // visit localhost:8000/monitoring to see asynqmon homepage
}
