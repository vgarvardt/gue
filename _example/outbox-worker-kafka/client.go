package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	gue "github.com/2tvenom/guex"
	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func newClientCommand() *cobra.Command {
	var gc *gue.Client

	return &cobra.Command{
		Use:   "client",
		Short: "Outbox Worker Client, enqueues messages to the gue for further processing by teh worker",
		PreRunE: func(cmd *cobra.Command, args []string) (err error) {
			gc, err = newGueClient(cmd.Context())
			return
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			quitCh := initQuitCh()
			for {
				select {
				case sig := <-quitCh:
					log.Printf("Received interrupt (%s), exiting app\n", sig.String())
					return nil

				case <-ctx.Done():
					log.Printf("Received context done (err: %q), exiting app\n", ctx.Err().Error())
					return nil

				default:
					var num int
					if err := survey.AskOne(
						&survey.Input{
							Message: "How many message should I publish to kafka using outbox?",
							Default: "10",
						},
						&num,
						survey.WithValidator(survey.Required),
					); err != nil {
						if err == terminal.InterruptErr {
							log.Printf("Received terminal interrupt, exiting app\n")
							return nil
						}
						return fmt.Errorf("could not get get response on how many message to publish: %w", err)
					}

					if num < 1 {
						log.Printf("Number of messages to publish must be greater than zero\n")
						continue
					}

					now := time.Now()
					for i := 0; i < num; i++ {
						msg := outboxMessage{
							Topic: kafkaTopic,
							Key:   []byte(fmt.Sprintf("%s-%d", now.String(), i%2)),
							Value: []byte(fmt.Sprintf("message #%d @ %s", i, now.String())),
							Headers: []sarama.RecordHeader{
								{Key: []byte("message-uuid"), Value: []byte(uuid.NewString())},
							},
						}

						args, err := json.Marshal(msg)
						if err != nil {
							return fmt.Errorf("could not marshal message to json: %w", err)
						}

						if err := gc.Enqueue(ctx, &guex.Job{
							Queue: outboxQueue,
							Type:  outboxJobType,
							Args:  args,
						}); err != nil {
							return fmt.Errorf("could not enqueue job: %w", err)
						}

						log.Printf(
							"Enqueued message for publishing to the gue: topic %q, key %q, value %q\n",
							msg.Topic, msg.Key, msg.Value,
						)
					}
				}
			}
		},
	}
}
