# Notification monitoring bottleneck and original-client retest

## Diagnosis and change

The failed `notification-test-198dcda69a` database was restarted alone for read-only diagnosis, with no application or Kafka replay. Its original `/metrics` aggregate scanned the delivery history on every drain poll. EXPLAIN ANALYZE measured 4287.092ms with 86,608 shared read blocks; a repeat measured 425.279ms with 86,576 shared read blocks (about 676MiB). PostgreSQL buffer reads are not necessarily physical disk reads because the OS may cache them.

`notification-service/database/schema.sql` now maintains five durable metric totals with statement-level transition-table triggers. Inserts, updates, deletes, truncation, rollback, duplicate events and duplicate ACKs preserve the totals transactionally. Initial installation bootstraps existing rows while writers are locked. Ingestion and sending update separate counter rows. `NotificationStore.metrics()` reads this bounded table instead of scanning delivery history. This adds transactional write overhead; it is not a stale in-memory cache.

The harness now performs a separate, single-snapshot full-table comparison after drain/replay, before accepting notification verification. It saves `metrics-audit.json` and an execution plan on success. This check was **not reached** in the retest below. Real PostgreSQL regression tests independently compare counters against base tables, including rollback and schema reinstallation.

## Unchanged workload and client

Run: `target/notification-test-b7b91085cc`.

```bash
python3 scripts/test-notifications.py --variant notification --subscribers 100 \
  --stations 10 --duration 60 --min-items 1 --max-items 20 --drain-timeout 600
```

No fixed-sales driver, no extended timeout, no smaller SKU subscription scope, no relaxed durability. The 0.25-second polling sleep is unchanged; faster queries naturally change the effective polling rate. Fifty subscribers follow all 2000 SKUs and fifty follow one SKU each. The nine teacher Java source files and two shell scripts are byte-identical to local upstream Git revision `0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06`; this is not a claim about the latest upstream main. Source verification and hashes are saved in the run directory.

## Results: checkout improves, overall notification validation incomplete

| Metric | Previous failed run | Counter-table retest |
|---|---:|---:|
| Completed load transactions | 6,956 | 11,562 |
| Original-client TPS | 115.8134 | 192.5378 |
| Start / scan / complete errors | 0 / 0 / 0 | 0 / 0 / 0 |
| Overall outcome | 600s drain timeout | Disk safety guard aborted drain |

The observed checkout TPS increased about 66.25% in this single-run comparison. It is not a repeated controlled benchmark or proof of restored historical baseline performance. The retest produced more sales and therefore more notification work, so it is not a fixed-notification-volume latency comparison.

The revised metrics query took **0.103ms execution time** during the live workload (`live-metrics-query-plan.json`; planning time 4.550ms). This isolates the removed monitoring scan, not all remaining bottlenecks.

The test started with approximately 9.29GiB free. During drain, the existing safety guard observed less than 1GiB free and aborted without deleting evidence. All five test PostgreSQL servers and application processes were shut down by the harness. After shutdown `df` reported approximately 4.1GiB free; this later reading does not invalidate the earlier guard result. The timeout remained 600 seconds.

Last saved drain snapshot:

- Expected sales 11,564 (11,562 load sales plus two recovery probes).
- Consumed sales 6,720; 4,844 not yet recorded by the notification service.
- Generated notifications 3,100,367; pending acknowledgements 302,361.
- Connected subscribers 100; send attempts 2,808,102.

After shutdown, subscriber logs contain 2,836,048 unique notification IDs, zero duplicate IDs per subscriber and zero malformed lines. Received-only latency is P50 106.43s / P95 394.53s / P99 472.16s. **These are censored partial statistics, not full-population latency or a successful routing/quantity audit.** Final outbox replay, full-delivery verification and the full-run counter audit were not reached.

## Remaining bottleneck and limits

Read-only live samples show delivery INSERT and dispatch UPDATE waiting on DataFileRead/DataFileWrite while autovacuum reads the same delivery table. At one sample it occupied about 1.56GB, had roughly 3.04 million estimated live rows and 3.71 million updates, with zero HOT updates. Delivery indexes accounted for over ten million cumulative PostgreSQL block reads; subscription heap reads exceeded two million. These cumulative statistics and samples support remaining database I/O/write-amplification contention, but do not prove a single replacement index or buffer setting will resolve it.

The counter fix removes a demonstrated shared bottleneck; it does **not** solve sustained fan-out capacity. Further work should profile the ingestion subscription join and random delivery-index updates, then test a covering subscription index and/or a separate compact dispatch-state table with equivalent durability and replay semantics. No untested storage redesign is included in this change. More disk headroom is required before another full run; do not disable the safety guard or merely increase the timeout.

## Validation and evidence

- 169 Java tests, zero failures/errors/skips, including real PostgreSQL tests.
- 31 Python tests passed, including the new independent metric-audit checks.
- Raw teacher report, `throughput.json`, `workload-validation.json`, `environment.json`, `failure.json`, `drain-status.json`, `console-output.log`, source verification, partial-delivery summary, live metrics plan and live storage/query samples are retained under the run directory.
- All subscriber JSONL logs and service/database/Kafka logs are retained; this retest's stopped database/Kafka data is also retained for investigation.
- After collecting the previous run's read-only query plans and table statistics, only its stopped PostgreSQL/Kafka data directories were removed. Its 355 other files, including reports and logs, were retained.

Changed implementation/test files: `notification-service/database/schema.sql`, `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`, `notification-service/src/test/java/edu/cs6510/notification/NotificationStorePostgresTest.java`, `notification-service/src/test/java/edu/cs6510/notification/NotificationBatchTest.java`, `scripts/test-notifications.py`; added `scripts/notification_metrics.py` and `scripts/tests/test_notification_metrics.py`. Neither the teacher client nor sibling event-driven-server was modified in this change.

Follow-up: after saving additional subscription and dispatch query plans, this run's stopped database/Kafka data was removed to make room for the next original-client retest. Reports and logs remain. See `notification-io-optimization.md` for the subsequent patch and successful full-delivery verification, including its unresolved latency/throughput limitations.
