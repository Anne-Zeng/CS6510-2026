# Subscription lookup and delivery-index I/O optimization

## Scope and diagnosis

Before cleanup, only the previous stopped test's notification database (`notification-test-b7b91085cc`) was restarted on diagnostic port 25446. No application or broker replay ran. Read-only EXPLAIN ANALYZE results were saved in `io-before.txt`; a covering-index experiment on this disposable database was saved in `io-covering-index.txt`. The database was stopped before deletion.

For a representative ten-SKU sale, the original join scanned all 100,050 subscription rows: 649 PostgreSQL shared read blocks and 23.683ms execution. Adding `(sku,subscriber_id)` changed it to an index-only lookup: 500 matched rows, 17 read blocks, 28 hits, two heap fetches and 0.205ms execution. These are sequential diagnostic measurements, not cold-cache controlled latency benchmarks; the important evidence is the changed access path and reduced pages visited.

The previous delivery table also had a 372MB unique index on `(event_id,subscriber_id,sku)`. Its statistics showed over three million index scans with no tuples returned for those scans. Every initial fan-out and non-HOT delivery-state update maintained this index alongside the notification-ID primary key and dispatch index. The pending fetch itself touched 1,063 buffers for 512 rows and took 86.013ms in the diagnostic sample. Wide, repeatedly updated delivery rows and dispatch-index churn remain a concern after this patch.

## Implementation and correctness boundary

Changed:

- `notification-service/database/schema.sql`: replace the single-column subscription index with `(sku,subscriber_id)`; remove the redundant delivery uniqueness constraint/index, including migration of existing default-named constraints.
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`: remove delivery-level `ON CONFLICT`, leaving the sales inbox deduplication gate unchanged.
- `notification-service/src/test/java/edu/cs6510/notification/NotificationStorePostgresTest.java`: add simultaneous replay and conflicting-payload/duplicate-SKU regression tests.

Exactly-once materialization is enforced through the supported transactional `accept` path: unique inbox event/transaction identity is inserted first, then delivery fan-out commits in the **same transaction**. A replay with matching payload skips fan-out; a conflicting payload fails. Subscription pairs remain unique, and `SaleCompleted` rejects duplicate SKUs. The existing advisory lock also serializes ingestion with subscription changes. Rollback, offline delivery, restart and replay semantics remain tested.

The removed constraint is no longer a guard against arbitrary direct SQL inserts into the delivery table. All delivery producers must use the transactional inbox path; future import/repair tools must preserve this boundary. The notification-ID primary key, foreign keys, owner checks, durable commits, ACK protocol and delivery history remain intact. No history is deleted on acknowledgement.

For an existing deployment, stop notification consumers, apply the schema and deploy the matching rebuilt service before restarting. Do not run the old service SQL (which names the removed ON CONFLICT target) concurrently with this migration. The assignment harness creates isolated databases and builds the matching code automatically.

## Cleanup

Verified no test-owned processes remained; each PostgreSQL data directory additionally returned stopped status from `pg_ctl`. Removed only the old run's five PostgreSQL directories and Kafka message-segment directory, plus the stopped dedicated regression-test database. Reports, subscriber JSONL files, service/broker/database text logs and diagnostic plans were retained. The cleanup manifest is `target/io-optimization-cleanup.json`; free space afterwards was 7.34GiB. Kafka's binary topic segments are temporary broker data, not the retained `kafka.log` text log.

## Original teacher-client retest

Run directory: `target/notification-test-6c346e80a0`.

```bash
python3 scripts/test-notifications.py --variant notification --subscribers 100 \
  --stations 10 --duration 60 --min-items 1 --max-items 20 --drain-timeout 600
```

Unchanged: original teacher client; 10 stations; 60 seconds; 1–20 items per basket; 100 subscribers; 2000 SKUs; half the subscribers follow all SKUs, half one SKU; 600-second drain limit; five PostgreSQL instances with 32MB shared buffers each; fsync and synchronous_commit enabled. No fixed-count driver or extended timeout. Nine Java source files and two client scripts match upstream Git revision `0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06` byte-for-byte. This is not a claim about latest upstream main.

| Metric | Result |
|---|---:|
| Original-client elapsed time | 60.0650405s |
| Load transactions | 7,321 |
| Checkout TPS | **121.8845** |
| Successful scans | 76,890 |
| Start / scan / complete errors | **0 / 0 / 0** |
| Sales including two recovery probes | 7,323 |
| Expected / received notifications | **3,434,049 / 3,434,049** |
| Missing / unexpected / wrong quantity / duplicates | **0 / 0 / 0 / 0** |
| Remaining pending notifications | **0** |
| Notification latency P50 | 91.043s |
| Notification latency P95 | **308.623s** |
| Notification latency P99 | 319.645s |
| Load + drain/replay/verification elapsed | 433.14s |

Independent delivery verification, offline delivery, restart, repeated completion, outbox replay and single-snapshot counter/full-table audit all passed. Final metrics-query execution time was 0.040ms. Notification percentiles now cover the complete verified population, unlike the previous aborted runs' received-only statistics.

## Interpretation and remaining work

This run **passes correctness and drains within the unchanged limit**, but does not meet the broader low-latency/no-throughput-regression goal. TPS is 36.70% below the previous counter-only run's 192.54 TPS, and notification P95 remains over five minutes. Compared with the earlier monitoring-scan failure's 115.81 TPS it is slightly higher, but this is not restored historical 264 TPS performance.

The runs have the same requested workload parameters, not the same completed-sale or notification counts. They are single local runs with low disk headroom, so do not attribute all TPS changes to one SQL change or present the latency difference as a controlled fixed-volume improvement.

During this retest, a read-only sample at approximately 6,600 consumed sales showed subscription heap reads of about 203,000 versus the previous sample's roughly two million, and delivery index reads of about 6.1 million versus ten million. Samples differ in timing and work completed; these are diagnostic indicators, not normalized performance claims. Delivery state still has zero HOT updates, large dirty/dead-tuple volume and a sizeable dispatch index. Faster fan-out moves the backlog toward dispatch and can increase contention with checkout on the same disk. A next measured experiment should separate compact dispatch state from immutable payload/history, or bound ingestion relative to dispatch capacity; neither untested change is included here.

## Validation and retained evidence

171 Java tests passed, including real PostgreSQL tests, with zero failures/errors/skips; 31 Python tests passed. The concurrent replay regression confirms three expected deliveries, not six or twelve, even with repeated events in both concurrent batches. Existing rollback, ownership, restart and counter regressions continue to pass.

All test services stopped successfully. This new run's reports, logs and database/Kafka data remain available. Evidence includes `verification.json`, `metrics-audit.json`, `metrics-query-plan.json`, `load-report/report-20261117-222310.json`, `throughput.json`, `environment.json`, `load-client-source-verification.json`, `live-storage-diagnostics.txt`, `console-output.log`, `java-test-summary.json`, `python-tests.log`, subscriber JSONL logs and service logs. The original client and sibling event-driven-server were not modified in this change.

Follow-up: this run's stopped database/Kafka data was subsequently cleaned for further experiments. Subscriber JSONL logs were losslessly gzip-archived with verified uncompressed SHA-256 hashes in `subscriber-log-archives.json`; reports and text logs remain. See `notification-throughput-latency.md` for the later results and current limitations.
