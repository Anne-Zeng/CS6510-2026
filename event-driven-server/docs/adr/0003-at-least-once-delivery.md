# ADR 0003 — Use at-least-once delivery with local idempotency

- Status: Accepted
- Scope: Scan events, broker redelivery and inventory Saga commands

## Context

A database commit cannot be atomically combined with Kafka publication or offset commit in this implementation. Process failure can occur after a write but before its acknowledgement. Retrying must not duplicate a scan or stock decrement.

## Decision

Commit successful basket updates and scan outbox entries together. Mark outbox entries published only after Kafka acknowledges their batch. Analytics commits event-ID deduplication and canonical events in its private database before the consumer callback permits an offset commit; a reused ID with a different payload is an error. Derived rankings are persisted asynchronously.

Store successful START/SCAN Kafka request results atomically with their local business writes. START uses one autocommitted PostgreSQL statement. SCAN defaults to a bounded writer queue of 512 requests, draining at most 64 already queued independent baskets without an intentional batching wait; the batch's basket changes, outbox events and saved responses commit in one statement. Existing results bypass writes. Independent missing/closed baskets retain their 404/409 outcomes. If the whole batch rolls back, retry each command individually with its original request ID. Single-request batches and disabled batching use the original atomic single-scan SQL. Batch diagnostics default off. See [ADR 0004](0004-durable-batched-scans.md).

The unique request-ID constraint fences racing duplicate statements: a losing statement rolls back all its writes, then reads the committed winner with a fresh snapshot. This avoids repeated inbox/advisory-lock calls and multi-statement transaction overhead. Redelivery of the same request ID returns its saved result. Inventory operations use the persisted Saga ID as their idempotency key; completion recovery reuses it after unknown outcomes.

Keep Kafka polling on consumer-owned threads and execute commands asynchronously on virtual threads. Each business service defaults to one command poller, and the adapter fallback is four command partitions. The accepted single-process deployment explicitly uses `KAFKA_TOPIC_PARTITIONS=1`; the setting remains configurable for additional consumers. Both command/reply and scan-event fetch waits are 500 ms with `fetch.min.bytes=1`, so available records return immediately while idle topics avoid continuous empty-fetch allocation observed in JFR. The command poll timeout defaults to 100 ms (`KAFKA_COMMAND_POLL_MS`). Cumulative stage diagnostics are opt-in (`KAFKA_PERFORMANCE_LOG=false` by default).

Serialize normal same-key commands within each process. Kafka producers retain `acks=all` and idempotence. A command completes only after local processing and broker acknowledgment of its correlated reply. Per-partition tracking advances offsets only through a completed prefix, even when unrelated handlers finish out of order. Commit those offsets asynchronously every 100 ms of poll-loop progress, and synchronize known completed prefixes during revocation/shutdown.

On failure, pause the affected partition, let its dispatched work finish, and seek to the earliest failed offset for replay. Ignore late completions belonging to revoked assignments. A failed offset commit can cause replay and relies on the same durable deduplication. Requesters wait directly for business replies while producer send failures complete their futures exceptionally; receiving a broker acknowledgment alone never completes a business request.

## Consequences

Publication and consumption remain at least once. There is no end-to-end exactly-once guarantee or distributed transaction. Same-key serialization applies to normal dispatch within a process; later work may have run before a failed record is replayed, and a revoked owner's workers may still finish. We do not claim strict ordering across faults, replicas or topics. Deduplication covers broker redelivery with the same IDs; an unrelated new HTTP request is not automatically the same command. The public API cannot supply the internal Kafka request ID. An accepted scan's six-second timeout reports an unknown outcome and does not remove potentially committing work or imply rollback. Queue-full/stopping conditions fail explicitly. Poisoned records and failed publications currently retry rather than entering a dead-letter queue; ordinary application rejection replies can complete normally. Durable inbox/outbox/command histories need a future retention policy, and replay/recovery behavior must be verified against a real broker.
