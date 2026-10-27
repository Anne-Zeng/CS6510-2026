# ADR 0004 — Batch already queued scans in one durable statement

- Status: Accepted
- Scope: Checkout scan persistence; service boundaries and the synchronous public API remain unchanged

## Context

Database diagnostics during load testing identified contention around durable WAL flushes. Scans dominate the workload, and a separate durable commit for each scan keeps repeating that work even after the single-command SQL path has removed extra database round trips. Increasing Kafka consumers does not remove this database cost. We need to evaluate fewer commits per group of independent scans while retaining durable acknowledgments, exact basket totals and replay safety.

## Decision

Enable `CHECKOUT_SCAN_BATCHING_ENABLED=true` by default. A single `ScanWriter` drains requests already waiting in its bounded 512-entry queue; it does not sleep to collect a larger batch. Each batch contains at most 64 distinct basket IDs and request IDs. Repeated baskets or IDs are deferred to later batches in local queue order. A one-request batch uses the single-scan statement, and setting the flag to false selects that original path for all scans.

Use one autocommitted PostgreSQL statement per batch. Acquire basket locks in a stable transaction-ID order, then persist line quantities, basket totals, outbox events and successful command results atomically. Each basket uses its existing line-price snapshot when appropriate. Existing command results are returned without another write. Missing or closed baskets receive independent 404/409 responses without discarding valid scans from the batch.

If a SQL constraint or concurrent duplicate rolls the statement back, retry its commands individually with their original IDs. A duplicate result insert rolls back the losing statement's business/outbox writes before a fresh read retrieves the committed result. The same fallback is safe after an unknown database response because committed IDs resolve to their saved responses. Never release a successful scan reply before its database call has committed.

Reject a full queue or stopping writer explicitly with 503. After accepting a scan, a six-second caller wait can expire while the write is still pending or committing; retain that work and report an unknown outcome. The original HTTP API cannot carry the internal Kafka command ID, so this deduplication does not make a newly issued HTTP scan an automatic retry of the original request. Shutdown rejects queued work explicitly and waits for the worker within its bounded shutdown budget.

Keep `CHECKOUT_SCAN_PERFORMANCE_LOG=false` normally. Enable it to inspect attempted batch sizes and write durations during diagnosis; use the unchanged Load Client and independent database audits to assess completed throughput and correctness.

## Alternatives and consequences

Keeping every scan in its own statement is simpler and remains available as an experiment. A deliberate batching timer could collect more work at low load but would add latency, so this design only drains existing queued work. Turning off durable commits would weaken the acknowledgment contract and is not part of this change.

Batching can amortize commit work across independent baskets, but the shared writer can add queueing latency and becomes a local serialization point. Batch SQL, failure isolation and lifecycle handling add complexity; malformed database work can force slower individual fallback. The queue is bounded, while a batch already removed from it can hold up to another 64 requests. Faults and cross-replica replay still use the at-least-once rules in [ADR 0003](0003-at-least-once-delivery.md).

The current verification run passed 113 tests, including 13 real-PostgreSQL tests covering statement atomicity, mixed basket outcomes and concurrent replica deduplication, plus writer lifecycle tests. These results do not establish a throughput gain or demonstrate that the previous Microservices transaction/TPS thresholds have been achieved. Updated real-broker recovery validation and audited performance measurements remain required before replacing the formal submission evidence.
