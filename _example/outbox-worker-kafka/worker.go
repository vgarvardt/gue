package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/2tvenom/gue/adapter"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

func newWorkerCommand() *cobra.Command {
	var (
		gc       *gue.Client
		producer sarama.SyncProducer
	)

	return &cobra.Command{
		Use:   "worker",
		Short: "Outbox Worker, reads gue messages enqueued by the client and publishes them to Kafka",
		PreRunE: func(cmd *cobra.Command, args []string) (err error) {
			gc, err = newGueClient(cmd.Context())
			if err != nil {
				return
			}

			if err = createTestTopic(); err != nil {
				return
			}

			producer, err = newSyncProducer()
			if err != nil {
				return
			}

			return
		},
		PostRunE: func(cmd *cobra.Command, args []string) error {
			return producer.Close()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			wm := gue.WorkMap{
				outboxJobType: outboxMessageHandler(producer),
			}

			worker, err := gue.NewWorker(
				gc, wm,
				gue.WithWorkerQueue(outboxQueue),
				gue.WithWorkerLogger(adapter.NewStdLogger()),
				gue.WithWorkerPollInterval(500*time.Millisecond),
				gue.WithWorkerPollStrategy(gue.RunAtPollStrategy),
				gue.WithWorkerID("outbox-worker-"+gue.RandomStringID()),
			)
			if err != nil {
				return fmt.Errorf("could not build gue worker: %w", err)
			}

			cancelCtx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			go func() {
				if err := worker.Run(cancelCtx); err != nil {
					log.Fatalf("Worker finished with error: %s\n", err)
				}
				log.Println("Worker finished")
			}()

			quitCh := initQuitCh()
			sig := <-quitCh
			log.Printf("Received interrupt (%s), exiting app\n", sig.String())
			cancel()
			return nil
		},
	}
}

func outboxMessageHandler(producer sarama.SyncProducer) gue.WorkFunc {
	return func(ctx context.Context, j *gue.Job) error {
		var m outboxMessage
		if err := json.Unmarshal(j.Args, &m); err != nil {
			return fmt.Errorf("could not unmarshal kafka oubox message: %w", err)
		}

		pm := sarama.ProducerMessage{
			Topic:   m.Topic,
			Key:     sarama.ByteEncoder(m.Key),
			Value:   sarama.ByteEncoder(m.Value),
			Headers: m.Headers,
		}
		partition, offset, err := producer.SendMessage(&pm)
		if err != nil {
			return fmt.Errorf("could not publish message to kafka from outbox [job-id: %d]: %w", j.ID, err)
		}

		log.Printf(
			"Published message to kafka: topic %q, partition %d, offset %d, key %q, value %q\n",
			m.Topic, partition, offset, m.Key, m.Value,
		)

		return nil
	}
}
