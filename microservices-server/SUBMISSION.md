# Self Checkout — Microservices Architecture

This submission is implemented in `microservices-server/`. Four independently packaged business services each own a PostgreSQL database. An independently deployable Gateway exposes the original client API. Services communicate over HTTP and do not access each other's tables.

| Service | Port | Owned data |
| --- | --- | --- |
| Catalog | 8081 | catalog_db: product directory |
| Checkout | 8082 | checkout_db: baskets, receipts, durable Sagas, scan outbox |
| Analytics | 8083 | analytics_db: event inbox, canonical events, windows and rankings |
| Inventory | 8084 | inventory_db: stock and idempotent reservation records |
| Gateway | 8080 | No database; routes public API requests |

The local deployment uses four separate databases on one PostgreSQL server; each service has its own database configuration and application account. Separate PostgreSQL servers can be configured through the service URLs. The old shared-persistence module and root database schema are historical references and are excluded from the build and initialization.

## Coordination across databases

Checkout orchestrates a durable Saga. It freezes a basket and persists an attempt ID before calling Inventory. Inventory atomically debits the entire basket and records CONFIRMED under that ID. Checkout then atomically completes its local transaction and Saga. Retrying the same ID cannot debit stock twice, and a changed payload is rejected. An unknown RPC outcome remains pending and is retried with the same ID after a crash or timeout. Database leases fence stale workers. Explicit rejection triggers cancellation and reopens the basket; committed stock is not undone because a response was lost. Previously persisted COMMITTING/RESERVED work remains recoverable.

This is coordinated eventual completion, not an atomic transaction spanning databases. Inventory can be committed while Checkout is temporarily pending; recovery completes the local receipt. A successful completion response means both sides are persisted.

Every successful scan atomically writes the basket and outbox in checkout_db. A background publisher delivers batches to Analytics over HTTP. Analytics atomically validates and deduplicates events in its own database before acknowledging. Lost acknowledgements cause safe replay. Rankings are eventually consistent; scans need not wait for Analytics availability.

Catalog lookup preserves the previous fixed-directory behavior: load one immutable snapshot through the Catalog API, then look up each scan by SKU in a local map. Directory changes require restarting Catalog and Checkout.

## Validation and measured results

The latest clean build passed all 43 component tests (Checkout 23, Analytics 8, Gateway 12), and the full isolated integration suite exited successfully. Build and integration validation are recorded in [validation.json](reports/submission/validation.json). The isolated integration suite uses temporary databases and ports, including inventory rejection, lost responses, restart recovery, stale leases, event replay, and Gateway/startup lifecycle checks.

| Workload | Stations / requested seconds | Completed transactions | Transactions/sec | Request errors | Completion P95 / P99 |
| --- | --- | ---: | ---: | ---: | --- |
| Default | 10 / 60 | 11,031 | 183.74 | 0 | 36.48 / 74.38 ms |
| Stress | 100 / 120 | 31,431 | 261.69 | 0 | 370.14 / 533.49 ms |

Both workload audits passed all 17 checks, including precise inventory decrement, basket totals, completed Saga state, event payload checksums, contiguous sequence numbers, and independently recomputed window rankings. Analytics was allowed to drain before the audits.

Compared with the previous submitted Service Based results (222.79 / 230.14 TPS), throughput changed by −17.53% / +13.71%. Completion tail latency increased in both workloads. These historical runs were not repeated under identical background load; no claim of universal performance improvement is made. See [PERFORMANCE.md](PERFORMANCE.md) for configuration, limitations, and raw evidence.

## Reproduce

From `microservices-server/`, prepare the four service databases and `.env` as described in [README.md](README.md), then run:

```bash
./mvnw clean package
python3 scripts/test-system.py
./scripts/run-benchmarks.sh \
  --modes default stress \
  --java-opts='-Xms64m -Xmx256m -XX:TieredStopAtLevel=4' \
  --checkout-inventory-max-in-flight 64 \
  --inventory-max-in-flight 8 \
  --publish-submission
```

Isolated tests and benchmarks require a local test account with CREATEDB and socket/localhost login access; configure SAGA_TEST_DB_USER / SAGA_TEST_DB_PASSWORD if necessary. They clean up temporary databases without resetting normal application databases. Start the application using `./run.sh`; individual service JARs and Gateway can also run independently with their own environment configuration. Do not commit `.env` credentials or generated build/log files.
