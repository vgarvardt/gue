package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"

	"github.com/vgarvardt/gue/v6"
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
			initLogger()

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
				gue.WithWorkerLogger(slog.Default()),
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
					slog.Error("Worker finished with error", slog.String("error", err.Error()))
				}
				slog.Info("Worker finished")
			}()

			quitCh := initQuitCh()
			sig := <-quitCh
			slog.Info("Received interrupt, exiting app", slog.String("signal", sig.String()))
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

		slog.Info("Published message to kafka",
			slog.String("topic", m.Topic),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
			slog.String("key", string(m.Key)),
			slog.String("value", string(m.Value)),
		)

		return nil
	}
}
