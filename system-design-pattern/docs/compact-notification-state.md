# Shared sale items and compact delivery state

## Implemented scope

The remaining write amplification was visible in the schema: each subscriber's mutable delivery row repeated event identity, transaction identity, SKU, quantity and completion time. Every send/ACK changed a row containing this repeated payload, while both the history primary key and pending-dispatch index remained active.

This change implements the first proposed storage-model step, **not** a subscriber-cursor redesign:

- `notification_sale_items`: one shared projection per `(event_id,sku)`, containing transaction ID, quantity and completion time. The service inserts it once and never updates it during dispatch or ACK processing. The canonical inbox payload is also retained for replay validation.
- `notification_deliveries`: notification ID, shared `item_id`, subscriber ID, attempts, retry time and ACK time. A bigint foreign key protects the shared reference; existing UUID notification IDs and subscriber ownership checks remain.
- Ingestion inserts shared items and fans out compact references in one data-modifying CTE, within the existing inbox transaction. Only newly inserted sales fan out. Failed fan-out rolls back inbox, shared items, states and counters together.
- Dispatch materializes at most 512 pending state rows first, then joins their shared payload by primary key. It preserves the previous ordering and wire fields. ACK/send updates do not touch shared payloads.

There is still one mutable state row per notification, and the pending index still changes on ACK/retry. This does not eliminate all fan-out writes or guarantee lower TPS cost. Shared projections are created for all items in accepted sales, including those without a matching subscriber.

## Migration and safety

`notification-service/database/schema.sql` now executes as one transaction. An advisory lock serializes schema installers, and exclusive table locks protect the backfill. For the legacy wide table it:

1. Creates shared items from the durable inbox payload.
2. Checks every old delivery's event/SKU, quantity, transaction ID and completion time against that shared data.
3. Backfills the reference, requires it to be non-null, then removes the repeated payload columns.
4. Preserves notification IDs, subscriber IDs, ACK timestamps, retry timestamps, attempts and metric totals.

**Stop notification writers/readers and back up the database before applying this migration to existing data.** This is an offline schema change: do not mix the old service binary with the compact schema. Mismatched legacy data aborts the transaction without dropping original columns. Reinstallation is idempotent. Large backfills need temporary disk/WAL headroom; this migration does not run VACUUM FULL and does not guarantee immediate shrinking of existing table files. The performance test below used fresh isolated databases, not a rewritten historical database.

The only databases migrated during development were dedicated test schemas/instances; no unrelated existing user database was changed.

## Correctness and measurements

Real PostgreSQL tests cover:

- One shared item copy across 100 subscribers, with unchanged shared-row versions after ACKs.
- Delivery-insert failure rolling back the shared items and inbox as well as delivery states.
- Legacy ID/ACK/retry preservation, idempotent migration and corrupt-payload migration rollback.
- Existing concurrent replay, conflicting identities, mixed-owner ACK rollback, offline/restart recovery and metric/table consistency.

**190 Java tests passed with zero failures/errors/skips**, including real PostgreSQL tests; **34 Python tests passed**. The dedicated migration regression evidence is in `target/compact-state-unit-jShyBT`.

The full test used the unchanged original teacher load-client:

```bash
python3 scripts/test-notifications.py --variant notification --subscribers 100 \
  --stations 10 --duration 60 --min-items 1 --max-items 20 --drain-timeout 600
```

Same workload/runtime settings as the previous candidate: 2000 SKUs, 1,000,000 opening stock per SKU, fifty all-SKU subscribers and fifty single-SKU subscribers, five separate PostgreSQL instances with 32MB buffers, tier-1 service JVMs, LZ4 WAL compression only for notification, and fsync/full-page writes/synchronous commit enabled. No timeout increase, smaller subscription scope or teacher-client modification.

Run: `target/notification-test-d9eccff4cc`.

| Metric | Previous `a64b2150b2` | Compact state `d9eccff4cc` |
|---|---:|---:|
| Load transactions | 11,971 | 11,309 |
| Checkout TPS | 199.3026 | **188.3708** |
| Start / scan / complete errors | 0 / 0 / 0 | **0 / 0 / 0** |
| Expected / received notifications | 5,656,556 / 5,656,556 | **5,267,152 / 5,267,152** |
| Notification P50 | 101.654s | 124.991s |
| Notification P95 | 253.126s | **188.924s** |
| Notification P99 | 257.603s | 231.099s |
| Missing / unexpected / wrong quantity / duplicates | 0 / 0 / 0 / 0 | **0 / 0 / 0 / 0** |
| Pending after drain | 0 | **0** |

All 11,311 sales including two recovery probes were consumed. Full delivery verification, counter audit, offline delivery, restart, repeated completion and outbox replay passed. Load plus drain/replay/verification took 355.69 seconds, not the denominator used for checkout TPS.

**P95 improved about 25.36%, but TPS was about 5.49% lower and P50 increased. The strict lower-latency/no-throughput-decline goal is not met.** `comparison.json` explicitly records `performance_goal_met:false`; the harness's `passed:true` means functional correctness, not performance acceptance. These are single runs at different times and different completed notification volumes, not a controlled fixed-volume or statistically significant comparison. Do not attribute the entire TPS change to the added join.

Storage evidence (`storage-snapshot.json`):

- 104,773 shared item rows serve 5,267,152 delivery states.
- A bounded 1024-row sample averaged approximately **100 bytes per compact row**, versus **188 bytes** for an equivalent reconstructed legacy row (about 47% narrower).
- Shared items and indexes occupied 26,607,616 bytes; delivery state and indexes occupied 2,099,208,192 bytes at the final snapshot. Logical row-width reduction is not a claim that total disk usage halved: indexes, MVCC and historical ACK state remain.

One live `EXPLAIN ANALYZE` (`live-storage-plan.txt`) returned 512 pending deliveries in **4.943ms**. It used the pending index first and then 512 shared-item primary-key lookups; those lookups used 1,536 buffer hits and no reads in that sample. The pending-state scan still read 437 buffers and wrote 115. Another sample in that file showed about 1.46 million estimated dead delivery tuples. This supports keeping attention on mutable state/index churn and queueing, rather than declaring the new payload join the sole bottleneck. It is one sample, not a query-latency distribution.

## Evidence and changed files

Reports and logs are retained under the run directory: raw load report, `verification.json`, `metrics-audit.json`, `storage-snapshot.json`, `live-storage-plan.txt`, `comparison.json`, source verification, implementation hashes, test summaries, console/service/database/broker logs and subscriber JSONL logs. Teacher Java source hashes recorded for the run match upstream revision `0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06`; build/run script bytes also match. This is not a statement about latest upstream main.

The previous run's subscriber logs were losslessly gzip-archived after checking uncompressed SHA-256 hashes; its reports/text logs remain. `subscriber-log-archives.json` records hashes and `gunzip subscribers/*.jsonl.gz` restores the old JSONL files. Only verified stopped temporary test database/Kafka data is cleaned.

After this run passed and all its processes stopped, its temporary PostgreSQL/Kafka data and the dedicated migration-test database were cleaned as well. Reports, query plans and logs remain; `temporary-data-cleanup.json` lists the removed data directories.

Implementation changes:

- `notification-service/database/schema.sql`
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`
- `notification-service/src/test/java/edu/cs6510/notification/NotificationStorePostgresTest.java`
- `notification-service/src/test/java/edu/cs6510/notification/NotificationBatchTest.java`
- `scripts/notification_metrics.py`, `scripts/stage_diagnostics.py`, `scripts/test-notifications.py`
- `scripts/tests/test_notification_metrics.py`

No checkout business code, teacher load-client, Kafka delivery contract or subscriber wire protocol was changed in this step. The next storage step would separate active pending state from acknowledged history or implement carefully bounded per-subscriber progress; that is **not implemented** by this patch.
