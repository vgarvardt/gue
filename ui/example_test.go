package asynqmon_test

import (
	"context"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/2tvenom/guex/database"
	asynqmon "github.com/2tvenom/guex/ui"
	"github.com/jackc/pgx/v4/pgxpool"
)

func Test_HTTPHandler(t *testing.T) {
	connPoolConfig, err := pgxpool.ParseConfig(os.Getenv("DB_DSN"))
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
