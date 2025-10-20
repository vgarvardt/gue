export CGO_ENABLED=0

.PHONY: deps-up
deps-up:
	docker compose up --detach --wait

.PHONY: deps-down
deps-down:
	docker compose down --volumes --remove-orphans

.start-client:
	@PG_PORT=$$(docker compose port postgres 5432 | cut -f2 -d":") && \
	DB_DSN="postgres://outbox:outbox@localhost:$$PG_PORT/outbox?sslmode=disable" \
	go run ./... client

.PHONY: client
client: deps-up .start-client

.start-worker:
	PG_PORT=$$(docker compose port postgres 5432 | cut -f2 -d":") && \
	KAFKA_PORT=$$(docker compose port redpanda 9092 | cut -f2 -d":") && \
	DB_DSN="postgres://outbox:outbox@localhost:$$PG_PORT/outbox?sslmode=disable" \
	KAFKA_BROKERS="localhost:$$KAFKA_PORT" \
	go run ./... worker

.PHONY: worker
worker: deps-up .start-worker
