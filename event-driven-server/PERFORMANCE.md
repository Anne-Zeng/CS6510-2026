# Kafka performance and correctness — interim evidence

**The requested performance target remains unmet.** Both transaction count and TPS must reach the previous Microservices report in each workload. The selected optimized candidate improved substantially over the initial Kafka run but remains below both thresholds. The formal files in `reports/submission/` are unchanged; new measurements are explicitly labeled **CANDIDATE — NOT SUBMISSION** in [reports/optimization](reports/optimization/README.md).

## Measured results

All dates below use America/Los_Angeles. Each Default workload uses 10 stations for 60 requested seconds; Stress uses 100 stations for 120 seconds. The original Load Client and basket size 1–20 are unchanged.

| Evidence / implementation | Default transactions / TPS | Stress transactions / TPS | Status |
| --- | ---: | ---: | --- |
| Previous Microservices formal baseline | 11,031 / 183.74143 | 31,431 / 261.68576 | Required minimum for each metric |
| Initial Kafka formal reports, October 26 | 426 / 6.91103 | 1,501 / 11.94916 | Existing submission evidence; below target |
| Earlier optimized batch candidate, October 27, source `load-20261027-034855` | 5,120 / 85.18933 | 15,112 / 125.22417 | Both workloads audited; below target |
| After the later batch SQL join simplification, source `load-20261027-035732` | 4,935 / 82.14601 | No valid report | Default audited; Stress invalidated by disk exhaustion |

The paired candidate's TPS is 12.33× / 10.48× the initial Kafka figures, but still 53.64% / 52.15% below Microservices. Both transaction-count targets also remain unmet. Its JAR hashes predate the final batch SQL join simplification. **Those paired results do not prove the performance of the current final artifacts.** The later audited Default result has its own environment/JAR hashes; no valid matching Stress result exists.

The selected candidate pair has zero request errors and passed all 17 independent database audits per workload. Copies preserve the original JSON bytes:

- [Default report](reports/optimization/report-default.json) and [audit](reports/optimization/correctness-default.json).
- [Stress report](reports/optimization/report-stress.json) and [audit](reports/optimization/correctness-stress.json).
- [Candidate environment](reports/optimization/environment.json), [database deployment](reports/optimization/database-deployment-default.json), and [baseline comparison](reports/optimization/comparison.json).
- [Provenance and SHA-256 manifest](reports/optimization/provenance.json), identifying source runs and implementation boundaries.
- [Later Default-only report](reports/optimization/latest-default/report-default.json), [audit](reports/optimization/latest-default/correctness.json), and [environment](reports/optimization/latest-default/environment.json).

The audits reconcile completed transactions with client acknowledgments; basket quantities/totals; exact nonnegative inventory decrements; confirmed Saga contents; outbox/inbox UUID and payload checksums; contiguous canonical scan sequences; and every due analytics window/ranking. They also check final-query/shutdown warnings and analytics catch-up. Passing these audits establishes correctness for the measured workloads, while the performance thresholds remain separate requirements.

## Candidate deployment and latency

The complete pair used five Java processes, four separate logical PostgreSQL databases on the local PostgreSQL server, and a native Apache Kafka 4.2.1 single-node KRaft broker at `127.0.0.1:19092`. Replication factor was 1. Command topics had four partitions and one command poller per business service; reply and scan topics had one partition each. Fetch waits were 500 ms with `fetch.min.bytes=1`; command poll timeout was 100 ms.

Each service used `-Xms64m -Xmx256m -XX:TieredStopAtLevel=4`. Database pools were Catalog 8, Checkout 16, Inventory 8 and Analytics 8; Checkout allowed 64 outstanding Inventory commands. Scan batching was enabled with queue capacity 512 and maximum batch size 64. `KAFKA_PERFORMANCE_LOG` was enabled for this diagnostic pair. The dataset contained 2,000 SKUs and 1,000,000 starting units per SKU. PostgreSQL `synchronous_commit` and `fsync` remained on, `commit_delay` was 0, and the pair inherited `en_US.UTF-8` database locale. The later Default-only experiment used temporary databases with locale `C`.

| Paired candidate workload | Actual seconds | Scans | START mean ms | SCAN mean ms | COMPLETE mean ms | COMPLETE P95 / P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 60.10142 | 54,127 | 5.30 | 8.81 | 18.86 | 43.44 / 83.39 |
| Stress | 120.67958 | 158,956 | 12.79 | 70.49 | 41.79 | 114.23 / 202.90 |

Scan latency still dominates the Stress transaction cycle: a basket averages about 10.5 scans. These are single local runs under different background load and deployment conditions. The combined changes and native broker cannot be assigned separate causal speedup figures from this pair.

## Findings and implemented changes

- The initial command inbox used several SQL calls and an explicit transaction for each scan. START/SCAN now persist successful command results together with business writes in atomic SQL. Duplicate command IDs still fence concurrent replay, and a losing statement rolls back its writes before reading the saved winner.
- The initial command poller processed one request at a time while waiting for business work and reply publication. Polling now dispatches independent keys to virtual-thread handlers. Per-partition offset tracking commits only a completed prefix after correlated replies receive broker acknowledgment, retaining failed work for replay.
- JFR identified substantial allocation from frequent empty fetches and many Kafka consumers. Defaults now use one command poller per service, four request partitions and the normal 500 ms empty-fetch wait with a one-byte minimum. Available messages can satisfy that minimum immediately. Stage diagnostics default off for normal runs.
- Database diagnostics identified contention around durable WAL flushes. `ScanWriter` coalesces already queued scans from up to 64 independent baskets in one durable statement without an intentional collection delay. Invalid baskets get individual errors, and an aborted batch falls back to the original command IDs. The later SQL change removes redundant joins; current throughput still requires a complete measured pair. See [ADR 0004](docs/adr/0004-durable-batched-scans.md).
- The runner now compares against the previous Microservices reports, separately identifies the older user-provided Service-Based reference, and refuses publication unless both workloads pass their audits and both transaction count and TPS meet the Microservices thresholds.

No durability setting was disabled to improve these measurements. Broker acknowledgment alone still does not complete a business request, and public HTTP completion waits for persisted Inventory/Checkout outcomes. HTTP retries cannot provide the internal Kafka request ID, so a timeout remains an unknown business outcome.

## Current verification and interrupted run

The current source passed **113 tests, including 13 real-PostgreSQL tests**. Coverage includes concurrent duplicate commands, mixed batch outcomes, atomic rollback, cross-replica replay, writer queue/timeout/shutdown behavior, API boundaries and offset-prefix tracking. Real PostgreSQL tests require their documented test connection variables; otherwise those tests are skipped.

**Updated real-Kafka recovery integration is still pending.** [The existing validation file](reports/submission/validation.json) records the earlier 81-test implementation and its historical real-broker outage/recovery suite. It is not evidence that the latest asynchronous/batched implementation has passed that suite.

At approximately 04:01 on October 27, the later Stress run exhausted host disk space and PostgreSQL/Kafka stopped. It produced no valid Stress JSON/audit and is excluded from performance evidence. PostgreSQL recovered after cleanup; about 1.4 GiB remained at that point. Further load and integration tests are halted until at least **4 GiB is free**. The benchmark runner now checks that reserve before starting and monitors a 2 GiB floor during load. No failed disk-full attempt is presented as a completed measurement.

After restoring that reserve, run current-source verification with Kafka/PostgreSQL available:

```bash
./mvnw package
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 python3 scripts/test-events.py
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 python3 scripts/benchmark-isolated.py --broker-deployment=native --publish-submission
```

The new runs must record their own artifact hashes and results. `--publish-submission` will retain the existing formal reports if either workload is below either threshold. The user request remains open until current-source recovery validation and both audited performance targets have been completed.
