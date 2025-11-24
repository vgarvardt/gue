/*
Package gue implements Go queues on top of PostgreSQL.
It uses transaction-level locks for concurrent work.

# PostgreSQL drivers

Package is using stdlib [database/sql] types internally and is tested with the following drivers
  - [github.com/jackc/pgx/v5]
  - [github.com/lib/pq]

# Usage

Here is a complete example showing worker setup for pgx/v5 and two jobs enqueued, one with a delay:

	package main

	import (
		"context"
		"encoding/json"
		"fmt"
		"log"
		"os"
		"time"

		"github.com/jackc/pgx/v5/pgxpool"
		"github.com/jackc/pgx/v5/stdlib"
		"golang.org/x/sync/errgroup"

		"github.com/vgarvardt/gue/v6"
	)

	type printNameArgs struct {
		Name string
	}

	func main() {
		printName := func(j *gue.Job) error {
			var args printNameArgs
			if err := json.Unmarshal(j.Args, &args); err != nil {
				return err
			}
			fmt.Printf("Hello %s!\n", args.Name)
			return nil
		}

		pgxCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
		if err != nil {
			log.Fatal(err)
		}

		pgxPool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
		if err != nil {
			log.Fatal(err)
		}
		defer pgxPool.Close()

		db := stdlib.OpenDBFromPool(pgxPool)

		gc, err := gue.NewClient(db)
		if err != nil {
			log.Fatal(err)
		}

		wm := gue.WorkMap{
			"PrintName": printName,
		}

		// create a pool with 2 workers
		workers, err := gue.NewWorkerPool(gc, wm, 2, gue.WithPoolQueue("name_printer"))
		if err != nil {
			log.Fatal(err)
		}

		ctx, shutdown := context.WithCancel(context.Background())

		// work jobs in goroutine
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			err := workers.Run(gctx)
			if err != nil {
				// In a real-world applications, use a better way to shut down
				// application on unrecoverable error. E.g. fx.Shutdowner from
				// go.uber.org/fx module.
				log.Fatal(err)
			}
			return err
		})

		args, err := json.Marshal(printNameArgs{Name: "vgarvardt"})
		if err != nil {
			log.Fatal(err)
		}

		j := &gue.Job{
			Type:  "PrintName",
			Args:  args,
		}
		if err := gc.Enqueue(context.Background(), j); err != nil {
			log.Fatal(err)
		}

		j := &gue.Job{
			Type:  "PrintName",
			RunAt: time.Now().UTC().Add(30 * time.Second), // delay 30 seconds
			Args:  args,
		}
		if err := gc.Enqueue(context.Background(), j); err != nil {
			log.Fatal(err)
		}

		time.Sleep(30 * time.Second) // wait for while

		// send shutdown signal to worker
		shutdown()
		if err := g.Wait(); err != nil {
			log.Fatal(err)
		}
	}
*/
package gue
