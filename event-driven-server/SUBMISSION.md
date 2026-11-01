# Self Checkout — Event Based Architecture

This implementation preserves the existing five deployable modules: Gateway, Catalog, Checkout, Inventory and Analytics. The Load Client uses the unchanged synchronous HTTP API. Every business interaction between services passes through Kafka: both request/reply commands and queries, and the asynchronous `ItemScanned` domain event. The shared `event-messaging` library wraps native Apache Kafka Java clients; it is not a sixth service.

## Assignment deliverables

| Requirement | Evidence |
| --- | --- |
| Default Load Client run: 10 stations, 60 seconds | `reports/submission/report-default.json` — measured; zero request errors; audit passed |
| Stress run: `--stations=100 --duration=120` | `reports/submission/report-stress.json` — measured; zero request errors; audit passed |
| Microservices and event broker diagram | [architecture.svg](docs/architecture.svg), with detailed [message flows](docs/architecture.md) |
| Source repository | Submit the URL of this repository containing `event-driven-server/` and the selected evidence |

Both reports were generated against this Kafka implementation on October 27, 2026 (America/Los_Angeles), and each passed all 17 correctness checks. See [PERFORMANCE.md](PERFORMANCE.md) for measured counts, latency, configuration and limits. Reports from earlier architectural versions are not Kafka benchmark evidence.

## Implemented architecture

Each business service owns a separate PostgreSQL database; Gateway owns none. Gateway maps the public routes to Kafka operations and waits for the actual business reply. Checkout obtains its fixed catalog snapshot through Kafka, coordinates inventory through Kafka, and publishes durable scan batches through Kafka. No service uses another service's HTTP API in the production business path. Local `/health` checks are diagnostic probes; retained internal controllers support diagnostics and compatibility tests and are not the production transport.

Checkout uses an **orchestrated durable Saga**. It freezes the basket, issues an idempotent inventory command keyed by the persisted Saga ID, and completes the local receipt after inventory success. Stock rejection and unknown outcomes have different recovery paths: rejection cancels the attempt and reopens the basket; a timeout preserves the attempt for retry. A successful HTTP completion response means both the inventory debit and Checkout completion are persisted.

A scan commits its basket update and outbox together. The outbox publisher waits for Kafka broker acknowledgement before marking delivery. Analytics deduplicates event IDs in a transactional inbox; Kafka offsets are committed only after the callback succeeds. A crash can replay messages, so delivery is **at least once**, with application idempotency. There is no distributed database/Kafka transaction and no end-to-end exactly-once claim.

Popular-item queries first obtain Checkout's scan checkpoint through Kafka. Analytics then waits within a bounded timeout for those scans and the due ranking windows, so the client's final query has a defined catch-up requirement. Normal scan requests remain independent of Analytics availability.

The local broker has a single KRaft node and replication factor 1. It demonstrates architecture and durability boundaries; it does not demonstrate broker high availability. See the [Kafka ADR](docs/adr/0001-use-kafka.md), [messaging/API ADR](docs/adr/0002-message-boundary-and-synchronous-api.md), and [delivery ADR](docs/adr/0003-at-least-once-delivery.md).

## Reproduce

Run these commands from `event-driven-server/`. Install Java 21, `javac` and PostgreSQL tools, and prepare the native Kafka distribution as described in [README.md](README.md). Keep PostgreSQL available for the runner's initial environment checks. Disable Low Power Mode on macOS and ensure at least 4 GiB of free disk space.

Build the service JARs:

```bash
./mvnw package
```

Start Kafka in one terminal and keep it running:

```bash
python3 scripts/kafka-local.py
```

After `Kafka Server started` appears, run the accepted benchmark configuration in a second terminal. The runner starts its own five service processes and four temporary PostgreSQL instances; do not start `./run.sh` alongside it.

```bash
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 KAFKA_TOPIC_PARTITIONS=1 \
  ./scripts/run-benchmarks.sh --broker-deployment=native --database-servers=separate \
  --java-opts='-Xms64m -Xmx256m -XX:TieredStopAtLevel=1' --publish-submission
```

Results are written to `reports/load-<timestamp>/`. The `--publish-submission` option updates `reports/submission/` only after validation succeeds. Omit that option for an exploratory run that keeps the submitted reports unchanged. See [PERFORMANCE.md](PERFORMANCE.md) for full deployment settings and validation details; see [README.md](README.md) for normal interactive service startup.

The runner invokes the unchanged Load Client once with default workload parameters and once with `--stations=100 --duration=120`, with only endpoint/report-output options added. It preserves raw JSON and performs independent database correctness audits before publishing both reports. The local test database account needs database creation/cleanup access; use `SAGA_TEST_DB_USER` and `SAGA_TEST_DB_PASSWORD` when necessary.

The current implementation passed 117 Java tests (including 13 real-PostgreSQL tests with no skips), 4 Python safety tests and the real-Kafka outage/replay/restart suite using a test-owned PostgreSQL instance. Both required workloads have zero request errors, pass all 34 database audits with these measured results: Default 15,866 / 264.23591 TPS; Stress 34,625 / 286.39168 TPS. See [validation.json](reports/submission/validation.json) and [PERFORMANCE.md](PERFORMANCE.md). Do not commit `.env`, credentials, generated JARs or runtime logs.
