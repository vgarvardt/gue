package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newWorkerCommand() *cobra.Command {
	var (
		gc       *guex.Client
		producer sarama.SyncProducer
	)

	return &cobra.Command{
		Use:   "worker",
		Short: "Outbox Worker, reads guex messages enqueued by the client and publishes them to Kafka",
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
			wm := guex.WorkMap{
				outboxJobType: outboxMessageHandler(producer),
			}

			worker, err := guex.NewWorkerPool(
				gc,
				guex.WithWorkerPoolQueue(guex.QueueLimit{
					Queue: outboxQueue,
					Limit: 10,
				}),
				guex.WithWorkerPanicWorkerMap(wm),
				guex.WithLogger(zap.NewNop()),
				guex.WithPoolInterval(500*time.Millisecond),
				guex.WithPoolID("outbox-worker-"+guex.RandomStringID()),
			)
			if err != nil {
				return fmt.Errorf("could not build guex worker: %w", err)
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

func outboxMessageHandler(producer sarama.SyncProducer) guex.WorkFunc {
	return func(ctx context.Context, j *guex.Job) error {
		var m outboxMessage
		if err := json.Unmarshal(j.Payload, &m); err != nil {
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
