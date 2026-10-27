# Inventory — Kafka commands and durable saga results

Consumes `INVENTORY_RESERVE`, `INVENTORY_GET`, `INVENTORY_CONFIRM`, `INVENTORY_RELEASE`, and `GET_LOW_STOCK` messages from `<prefix>.inventory.requests`. Results return through Kafka with the original correlation ID.

Inventory owns its PostgreSQL database. New reservations atomically debit the entire basket and save CONFIRMED, using the saga ID as the durable idempotency key. Repeating that saga does not debit again; different contents with the same ID are rejected. Ordered locks and constraints prevent partial or negative stock.

An absent reply is an unknown outcome: Checkout retries the same saga and never interprets a broker ACK as completed business work. Compatibility confirm/release operations preserve the existing saga recovery behavior.

Local HTTP endpoints are health/diagnostic interfaces; application services communicate through Kafka. See [architecture](../docs/architecture.md) and [integration testing](../README.md).
