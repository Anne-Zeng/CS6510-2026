# Checkout — Kafka commands, durable saga and scan outbox

Checkout only reads/writes `checkout_db`. It requests a Catalog snapshot through
Kafka, sends Inventory command/result messages through Kafka, and publishes
`ItemScanned` batches to the scan topic. No service HTTP client remains.

The public Gateway retains the original synchronous HTTP contract. Its Kafka
commands are `START_TRANSACTION`, `GET_TRANSACTION`, `SCAN_ITEM`, and
`COMPLETE_TRANSACTION`. `SCAN_CHECKPOINT` returns the durable number of scan
outbox rows, allowing Analytics queries to wait for the corresponding events.

## Delivery and transaction boundaries

- `START_TRANSACTION` and `SCAN_ITEM` store the Kafka envelope request ID and
  response in `command_results`, in the same local transaction as business writes.
  Replaying an envelope after a lost reply returns the same result without
  creating another basket or scanning twice. Distinct intentional scans have
  distinct envelope IDs.
- Concurrent redeliveries serialize on a PostgreSQL transaction advisory lock.
  The catalog snapshot is resolved before entering the inbox transaction, so no
  database connection is held while waiting for a Catalog response.
- Inventory operations use the durable saga ID as their partition/idempotency key.
  `INVENTORY_RESERVE` atomically debits the basket and returns `CONFIRMED`. A
  definitive rejection transitions through `CANCELLING`; unknown results retain
  `RESERVING` and retry the same ID. Legacy `COMMITTING`/`RESERVED` states remain
  recoverable through `INVENTORY_CONFIRM` and `INVENTORY_RELEASE`.
- Inventory confirmation precedes local Checkout completion. HTTP 200 therefore
  means both have persisted. Duplicate completion returns the original receipt.
  A timeout may return 503 while durable recovery continues; retry the same basket.
- Scan writes and outbox insertion are atomic. The outbox publisher marks rows
  processed only after Kafka broker acknowledgement. A crash after broker ack
  can republish UUIDs; Analytics deduplicates them in its transactional inbox.
  Broker acknowledgement does not mean Analytics has finished consuming.

Saga leases, backoff, and recovery are retained. Inventory command concurrency
is configurable using `CHECKOUT_INVENTORY_MAX_IN_FLIGHT` (default 64). Outbox
polling uses `CHECKOUT_OUTBOX_POLL_MS` (default 250 ms). Kafka configuration comes
from the shared event-messaging module; see the project README and `.env.example`.

## Database and verification

Use `database/schema.sql` for a new or existing checkout database. It preserves
existing data and includes `command_results`; the individual upgrade is
`database/005-command-inbox.sql`. A separate database from the previous assignment
is recommended for independent load-test results.

From `event-driven-server`:

```bash
./mvnw -pl checkout-service -am test
```

Tests cover catalog caching/retries, durable saga recovery after unknown Inventory
outcomes, Inventory command validation, scan batch identity, and outbox ordering.
See the project-level scripts for Kafka integration and load-client verification.
