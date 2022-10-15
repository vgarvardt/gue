package main

import (
	"context"
	"log"

	"github.com/spf13/cobra"
)

func main() {
	ctx := context.Background()

	rootCmd := &cobra.Command{
		Use:     "outbox [command]",
		Version: "0.0.0-example",
	}

	rootCmd.AddCommand(newClientCommand())
	rootCmd.AddCommand(newWorkerCommand())

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
