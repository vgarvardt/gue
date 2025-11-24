# Gue-based Outbox Worker for Kafka

> **Disclaimer**: this Outbox Worker implementation is just a showcase example of gue library usage and is not designed
> to be used in high load environments. There are ways of improving its performance and resilience, but they are out of
> scope of this showcase example.

This is simple [Transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
implementation that uses `gue` for messages store/relay processes.

In order to run this example you'll need to have the following tools preinstalled in your system:

- Go 1.24+
- Docker with compose command - run `docker compose version` to ensure it works
- make

Example consist of two components:

## 1. Client

Generates messages and enqueues them as `gue` Jobs for further processing. Run it with `make client`.

Once running it asks how may messages do you want to publish to Kafka. Give it a number and check that jobs are being
inserted into the `gue_jobs` table in the database.

## 2. Worker

Runs `gue` Worker, polls Jobs enqueued by the Client and tries to publish them to Kafka. Run it with `make worker`.

Once running it polls jobs from the `gue_jobs` table in the database and tries to publish messages to kafka.

### A note on Kafka

To avoid spinning up real [Kafka](https://kafka.apache.org/), [Redpanda](https://redpanda.com/) instance is used - it
uses Kafka-compatible protocol and consumes much less resources. Everything else is not so important for the purpose of
this showcase example.
