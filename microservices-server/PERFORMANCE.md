# Microservices performance and correctness

## Formal run

Source run: `reports/load-20261025-000344`, started October 25, 2026 at 00:03 Pacific time. Selected evidence is preserved in `reports/submission/`. Each workload was measured once; the historical Service Based submission was not rerun concurrently on an otherwise identical host.

| Workload | Stations / seconds requested | Actual seconds | Completed | Scans | TPS | Errors | Completion P95 / P99 (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| Default | 10 / 60 | 60.03545 | 11,031 | 115,318 | 183.74 | 0 | 36.48 / 74.38 |
| Stress | 100 / 120 | 120.10971 | 31,431 | 330,946 | 261.69 | 0 | 370.14 / 533.49 |

TPS = totalTransactions / wallClockSeconds. Items/sec = totalItemsScanned / wallClockSeconds. Use actual elapsed time rather than the requested duration.

## Comparison with the previous submission

| Workload | Historical completed / TPS | Current completed / TPS | TPS change | Historical completion P95 / P99 (ms) |
| --- | --- | --- | ---: | --- |
| Default | 13,392 / 222.79 | 11,031 / 183.74 | −17.53% | 14.32 / 31.09 |
| Stress | 27,691 / 230.14 | 31,431 / 261.69 | +13.71% | 134.69 / 227.55 |

Stress throughput improved, while Default throughput fell and completion tail latency increased in both workloads. Additional HTTP communication and independently durable database steps remain part of the microservices design. This run alone does not establish the cause of the remaining performance gap; background load and random baskets were not controlled across historical runs.

`comparison.json` also contains another local historical baseline (166.40 / 186.40 TPS), with its source paths and timestamps. Its +10.42% / +40.39% changes must not be presented as comparisons with the previous submitted 222.79 / 230.14 TPS results. The user-provided submission reference is separately recorded in reports/comparison/service-based-user-reference-20261016.json.

## Actual configuration

macOS ARM64, 8 logical CPUs; Java 22.0.1; PostgreSQL 15.19. Four private databases shared one PostgreSQL server. Each JVM used `-Xms64m -Xmx256m -XX:TieredStopAtLevel=4`. Spring virtual threads were disabled. Catalog/Checkout/Analytics/Inventory pools were 8/16/8/8; Checkout inventory RPC concurrency was 64 and Inventory's ordinary operation permit limit was 8. New inventory debits use the batch writer and do not hold that permit while waiting. Initial data: 2,000 SKUs, 1,000,000 units per SKU, 1–20 items per basket. Outbox batches contain up to 500 events.

Durability remained enabled: synchronous_commit=on and fsync=on; commit_delay=0. Database collation remained en_US.UTF-8; no collation optimization is claimed. Exact environment, source/client/schema hashes and measured JAR hashes are preserved in environment.json. Rebuilding JARs later can change archive hashes without changing the historical benchmark evidence.

## Implemented optimizations and limits

- One Catalog API snapshot followed by local SKU lookup, rather than fetching the full directory on every scan.
- Atomic single-statement basket/outbox writes and frozen basket reuse during checkout.
- Inventory debit and confirmation in one idempotent remote step; local Checkout completion remains recoverable. Legacy two-step work is supported.
- Inventory's writer batches up to 64 baskets into one SQL statement, with ordered stock locking and per-basket fallback after batch rejection. Its queue is currently unbounded; upstream RPC concurrency limits this deployment's normal pressure, but this is not a global multi-instance admission limit.
- Reused HTTP connections and a keep-alive Gateway proxy; batched Analytics inbox/canonical inserts, outbox acknowledgements and ranking writes.

These preserve private stores, idempotency and recovery. Unknown outcomes are retried using the same durable attempt ID, rather than abandoning a potentially committed inventory operation.

## Correctness evidence

The final clean build passed 43/43 component tests, and the full isolated integration suite passed, including Analytics outage/replay and run.sh cleanup/restart. These checks were rerun after the benchmark; production Java and SQL were unchanged during documentation finalization.

Both workloads passed 17/17 independent checks: client/database completed counts, exact stock decrement, correct basket totals, no unfinished Sagas or reservations, outbox/event payload checksums, contiguous event sequences, and every due ranking window. Default published 230 due windows; Stress published 661. Analytics catch-up took 1.91 seconds and 33.92 seconds respectively after load, so final audit success does not imply synchronous analytics visibility.

- [Default report](reports/submission/report-default.json) and [audit](reports/submission/correctness-default.json)
- [Stress report](reports/submission/report-stress.json) and [audit](reports/submission/correctness-stress.json)
- [Environment](reports/submission/environment.json) and [baseline comparisons](reports/submission/comparison.json)
- [Default database settings](reports/submission/database-deployment-default.json) and [Stress settings](reports/submission/database-deployment-stress.json)
- [Build and integration validation](reports/submission/validation.json)

For reproduction, use the exact command in [SUBMISSION.md](SUBMISSION.md). Historical diagnostic and validation files may remain locally, but only the selected evidence listed here is part of the final submission.
