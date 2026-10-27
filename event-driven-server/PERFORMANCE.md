# Kafka performance and correctness

Measured on October 26, 2026 (America/Los_Angeles). Source run: `reports/load-20261026-225303`. The two submission JSON files are byte-for-byte copies of the original Load Client outputs; earlier unsuccessful development runs remain local and are not submission reports.

| Workload | Stations | Requested seconds | Actual seconds | Transactions | Scans | TPS | Request errors | Complete P95 / P99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 10 | 60 | 61.64 | 426 | 4,470 | 6.91 | 0 | 468.71 / 1110.77 |
| Stress | 100 | 120 | 125.62 | 1,501 | 15,794 | 11.95 | 0 | 2311.53 / 4646.79 |

Both workloads passed all 17 independent database audits. Checks cover completed baskets versus client acknowledgements, exact nonnegative stock decrements, confirmed Inventory Saga contents, basket totals, complete outbox/inbox UUID and payload agreement, contiguous canonical scan sequences, and an independent recomputation of every due analytics window. Both immediate final client queries completed without warnings. See [correctness-default](reports/submission/correctness-default.json), [correctness-stress](reports/submission/correctness-stress.json), and [environment](reports/submission/environment.json).

## Tested configuration

- Five Java processes, four separate logical PostgreSQL databases on one local PostgreSQL server, and Apache Kafka 4.2.1 in Docker on the same 8 GiB ARM laptop.
- Kafka: single-node KRaft, replication factor 1, 24 partitions per command topic, one partition for replies and scan batches. Command consumers: Checkout 24, Inventory 4, Catalog 1, Analytics 1.
- JVM per process: `-Xms32m -Xmx160m -XX:TieredStopAtLevel=1`; service connection pools: Catalog 8, Checkout 16, Inventory 8, Analytics 8.
- 2,000 SKUs; 1,000,000 initial units per SKU. Each workload starts with four fresh temporary databases and a separate Kafka namespace; PostgreSQL durable commit/fsync remain enabled.
- Original Load Client sources are unchanged. Basket size 1–20 and the default 10-second client timeout remain unchanged. `--baseUrl` and `--reportDir` only select the isolated instance and output directory.

Reproduce the measured workload order from the project directory with Kafka and PostgreSQL available:

```bash
./mvnw package
python3 scripts/benchmark-isolated.py --modes stress default --publish-submission
```

The runner removes only its temporary databases, preserves raw reports locally, and publishes only when both workloads pass every audit. Normal server startup is described in [README.md](README.md).

## Functional and recovery validation

All 81 component tests passed. The separate real Kafka integration suite passed API route/error-shape checks, concurrent repeated completion, Analytics outage and duplicate UUID replay, and Inventory outage followed by Checkout restart and durable Saga recovery. Its independent final audit verified three completed baskets, 503 unique scans, one ranking window, exact stock totals, and no active Saga. See [validation.json](reports/submission/validation.json).

The fault suite ran with 12 Checkout consumers before the final increase to 24; the final two audited load runs validate the 24-consumer configuration. Offset processing and recovery logic are identical. Successful callbacks submit offsets asynchronously; shutdown commits only explicitly completed offsets. Reply consumers do not persist process-local correlation offsets.

## Interpretation limits

These are single local runs under the host's existing background load. The server settings prioritize fitting five JVMs plus Kafka on this laptop. Compared with the historical HTTP implementation, this deployment has substantially higher latency and lower throughput; the numbers do not establish a causal architecture comparison. Historical figures in `comparison.json` identify separate implementations and run times.

Correlated Kafka requests/replies preserve the synchronous public API, and scan propagation is asynchronous. Publisher idempotence and application deduplication do not imply end-to-end exactly-once delivery. The single broker does not demonstrate high availability. Inbox/outbox/command histories currently have no retention policy; poison records retry rather than going to a dead-letter queue.
