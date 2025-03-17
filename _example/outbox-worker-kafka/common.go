package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sadpenguinn/gue/v6"
	"github.com/sadpenguinn/gue/v6/adapter"
	"github.com/sadpenguinn/gue/v6/adapter/pgxv5"
)

// func init() {
//	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
// }

const (
	kafkaTopic    = "test-topic"
	outboxQueue   = "outbox-kafka"
	outboxJobType = "outbox-message"
)

type outboxMessage struct {
	Topic   string                `json:"topic"`
	Key     []byte                `json:"key"`
	Value   []byte                `json:"value"`
	Headers []sarama.RecordHeader `json:"headers"`
}

func initQuitCh() chan os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(
		sigCh,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	return sigCh
}

func newGueClient(ctx context.Context) (*gue.Client, error) {
	dbDSN := os.Getenv("DB_DSN")
	if dbDSN == "" {
		return nil, errors.New("DB_DSN env var is not set, should be something like postgres://user:password@host:port/dbname")
	}

	log.Printf("Connecting to the DB %q\n", dbDSN)
	connPoolConfig, err := pgxpool.ParseConfig(dbDSN)
	if err != nil {
		return nil, fmt.Errorf("could not parse DB DSN to connection config: %w", err)
	}

	connPool, err := pgxpool.NewWithConfig(ctx, connPoolConfig)
	if err != nil {
		return nil, fmt.Errorf("could not connection pool: %w", err)
	}

	if err := applyGueMigration(ctx, connPool); err != nil {
		return nil, err
	}

	guePool := pgxv5.NewConnPool(connPool)
	gc, err := gue.NewClient(
		guePool,
		gue.WithClientID("outbox-worker-client-"+gue.RandomStringID()),
		gue.WithClientLogger(adapter.NewStdLogger()),
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate gue client: %w", err)
	}

	return gc, nil
}

func applyGueMigration(ctx context.Context, connPool *pgxpool.Pool) error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("could not get current working directory: %w", err)
	}

	schemaPath := path.Join(cwd, "..", "..", "migrations", "schema.sql")
	queries, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("could not read schema file contents: %w", err)
	}

	if _, err := connPool.Exec(ctx, string(queries)); err != nil {
		return fmt.Errorf("could not apply gue schema migration: %w", err)
	}

	return nil
}

func createTestTopic() error {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		return errors.New("KAFKA_BROKERS env var is not set, should be something like localhost:9092")
	}

	log.Printf("Initialising test kafka topic at %q\n", kafkaBrokers)
	config := sarama.NewConfig()
	config.ClientID = "gue-outbox-worker-kafka-example-admin"

	ca, err := sarama.NewClusterAdmin(strings.Split(kafkaBrokers, ","), config)
	if err != nil {
		return fmt.Errorf("could not create kafka cluster admin client: %w", err)
	}

	if err := ca.CreateTopic(kafkaTopic, &sarama.TopicDetail{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, false); err != nil {
		var topicErr *sarama.TopicError
		if !errors.As(err, &topicErr) || topicErr.Err != sarama.ErrTopicAlreadyExists {
			return fmt.Errorf("could not create test topic: %w", err)
		}
	}

	if err := ca.Close(); err != nil {
		return fmt.Errorf("could not properly close kafka cluster admin client: %w", err)
	}

	return nil
}

func newSyncProducer() (sarama.SyncProducer, error) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		return nil, errors.New("KAFKA_BROKERS env var is not set, should be something like localhost:9092")
	}

	config := sarama.NewConfig()
	config.ClientID = "gue-outbox-worker-kafka-example"

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true

	log.Printf("Initialising sync kafka producer at %q\n", kafkaBrokers)
	producer, err := sarama.NewSyncProducer(strings.Split(kafkaBrokers, ","), config)
	if err != nil {
		return nil, fmt.Errorf("coulf not instantiate new sync producer: %w", err)
	}

	return producer, nil
}
