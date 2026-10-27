# ADR 0003 — Use at-least-once delivery with local idempotency

- Status: Accepted
- Scope: Scan events, broker redelivery and inventory Saga commands

## Context

A database commit cannot be atomically combined with Kafka publication or offset commit in this implementation. Process failure can occur after a write but before its acknowledgement. Retrying must not duplicate a scan or stock decrement.

## Decision

Commit successful basket updates and scan outbox entries together. Mark outbox entries published only after Kafka acknowledges their batch. Analytics commits event-ID deduplication and canonical events in its private database before the consumer callback permits an offset commit; a reused ID with a different payload is an error. Derived rankings are persisted asynchronously.

Store successful START/SCAN Kafka request results atomically with their local business writes. Redelivery of the same request ID returns that result. Inventory operations use the persisted Saga ID as their idempotency key; completion recovery reuses it after unknown outcomes. Kafka producers use `acks=all` and idempotence, and consumer callbacks retry before advancing offsets. Successful work uses asynchronous offset commits to avoid per-message coordinator waits; failed commits can replay work and rely on the same local deduplication. Shutdown commits explicit completed offsets, never a polled but unfinished message.

## Consequences

Publication and consumption remain at least once. There is no end-to-end exactly-once guarantee or distributed transaction. Deduplication covers broker redelivery with the same IDs; an unrelated new HTTP request is not automatically the same command. Rejected or poisoned messages currently retry rather than entering a dead-letter queue. Durable inbox/outbox/command histories need a future retention policy, and replay/recovery behavior must be verified against a real broker.
