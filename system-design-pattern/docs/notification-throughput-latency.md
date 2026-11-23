# Throughput and notification latency: measured optimization results

## Outcome and limits

Final original-client run: `target/notification-test-a64b2150b2`.

| Metric | Previous completed run `6c346e80a0` | Final run `a64b2150b2` |
|---|---:|---:|
| Load transactions | 7,321 | 11,971 |
| Checkout TPS | 121.8845 | **199.3026** |
| Start / scan / complete errors | 0 / 0 / 0 | **0 / 0 / 0** |
| Expected and received notifications | 3,434,049 | **5,656,556** |
| Notification P50 | 91.043s | 101.654s |
| Notification P95 | 308.623s | **253.126s** |
| Notification P99 | 319.645s | 257.603s |
| Missing / unexpected / wrong quantity / duplicates | 0 / 0 / 0 / 0 | **0 / 0 / 0 / 0** |
| Pending after drain | 0 | **0** |

TPS increased **63.52%** and P95 decreased **17.98%**, while notification volume increased approximately **64.72%**. P50 increased, so this is not an improvement at every percentile. All 11,973 sales, including two recovery probes, were consumed. Independent full-delivery verification, counter/full-table audit, offline delivery, service restart, repeated completion and outbox replay passed. Load plus drain/replay/verification took 392.68s; that is not pure drain time or the denominator of checkout TPS.

This is a single-run improvement over the previous notification run and also exceeds the earlier 192.54 TPS counter-only result. **It does not restore the historical event-driven-only 264.24 TPS baseline, and a 253-second P95 is still high.** The runs use identical requested workload parameters but different completed-sale and notification counts, not fixed-volume or fixed-arrival-rate traffic. Do not claim statistical significance, isolate one change's effect from these runs, or present the historical no-notification server as an equivalent workload.

## Why throughput fell and latency grew

Fifty subscribers follow all 2000 SKUs and fifty follow one SKU. A checkout sale therefore fans out to hundreds of persisted notifications, not just one message. The previous 60-second run generated 3.43 million notification rows. Async processing removes synchronous notification waiting from checkout, but not competition for the same laptop's CPU, memory, disk and PostgreSQL writes.

The measured evidence identifies several costs:

- Ingestion previously used two SQL calls per sale even inside a 16-sale transaction, and interleaved different subscribers' rows across sales.
- Random UUIDv4 delivery keys spread insertion and ACK-update index work across a large history. Updates are not HOT because indexed dispatch state changes. Earlier samples recorded millions of delivery-index reads and large autovacuum/write traffic.
- The subscriber previously flushed its log and sent an ACK for each notification; delivery also used one WebSocket message per notification. Millions of small frames and writes compete with checkout processing.
- A cache/JIT exploration observed 4,886,648,989 WAL bytes, 397,949 full-page images and eight requested checkpoints. Both ingestion and dispatch appeared in DataFileWrite waits. These are snapshots, not a normalized cross-run cost comparison.
- The host has 8GiB RAM and a sampled swap usage near 2GiB. Power checks showed AC power and Low Power Mode off; no thermal warning was recorded. These observations do not prove swapping or a single query caused all throughput variation.

The final run's mean SCAN_ITEM latency fell from 6.709ms in the previous completed run to 3.718ms, consistent with reduced contention. Faster checkout also generates more notification work, which explains why a short-latency intermediate run cannot be extrapolated to the final higher-throughput workload.

## Changes retained

1. **True bulk ingestion.** Validate every sale, insert the entire inbox batch once, validate conflicting/replayed identities, then fan out only newly inserted sales in one SQL statement ordered by subscriber. The existing 16-sale chunk limit, transaction boundary, subscription cutoff and replay semantics remain. A conflict or fan-out database failure rolls back the whole transaction.
2. **Time-prefixed delivery IDs.** New notifications use UUIDv7-shaped IDs with a 48-bit millisecond timestamp and 74 random bits, following [RFC 9562, section 5.7](https://www.rfc-editor.org/rfc/rfc9562.html#section-5.7). PostgreSQL 15 generates them using its existing random UUID function plus timestamp bytes. This improves key locality without changing the UUID wire representation. Existing UUIDv4 IDs are not rewritten and remain acknowledgeable. There is no promise of strict ordering within a millisecond or across clock rollback; business ordering does not depend on that promise.
3. **Bounded log-before-ACK batches.** The subscriber records individual JSONL entries and flushes before acknowledging up to 32 IDs, with a 20ms flush trigger for partial batches. Scheduling and network stalls can exceed this trigger; it is not a hard delivery deadline. UUID objects reduce deduplication-set memory compared with retained ID strings. A failed log flush sends no ACK. Lost ACKs/reconnects do not duplicate log entries. Flush has the same OS-buffer semantics as the old client; this is not a new per-record fsync guarantee.
4. **Opt-in delivery batches.** `SUBSCRIBE` can request `batchDeliveries:true`. The server then sends `SALE_BATCH` messages with at most 32 complete individual delivery records. Legacy subscribers still receive individual `SALE` messages; legacy `ACK` remains supported alongside bounded `ACKS`. The subscriber has four-frame demand and an eight-frame queue, validates every SKU/owner and logs every notification independently. The teacher load-client is unrelated to this subscriber client and remains unchanged.
5. **Lossless WAL compression in the isolated runner.** Only the notification PostgreSQL instance now uses `wal_compression=lz4`. `fsync`, `synchronous_commit` and `full_page_writes` remain on. All instances retain 32MB shared buffers and service JVMs retain the original 64–256MB heap / tier-1 compilation settings. The unsuccessful 128MB/C2 defaults were reverted. Actual settings are retained in `database-runtime-evidence.txt`; `environment.json` records per-instance WAL settings. Fixed-volume comparison tooling now rejects differing or unknown WAL/durability metadata instead of calling those runs equivalent.

An existing deployment must rebuild both notification service and subscriber client to use batch transport, and apply the schema migration for new ordered IDs. Old IDs remain intact. The WAL setting is applied by the isolated test runner, not automatically to an unrelated external PostgreSQL server. That server must be configured separately if the same tuning is desired. LZ4 requires a PostgreSQL build with LZ4 support; the tested Homebrew PostgreSQL 15.19 build has it.

## Same workload, preserved experiments

All workload-bearing runs used the teacher client's nine Java source files, byte-identical to local upstream revision `0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06`. Build/run scripts also match. Recorded source hashes were checked; this is not a claim about latest upstream main.

| Run suffix | Candidate | TPS | Full-population P95 | Outcome |
|---|---|---:|---:|---|
| `621ab31d21` | Bulk SQL | 118.72 | 304.22s | Full audit passed |
| `ef78a9f094` | Bulk SQL + log/ACK batches | 114.97 | 209.85s | Full audit passed |
| `9a9573adea` | 128MB notification cache + C2 | 112.49 | Not available | Agent cancelled regressing exploration before full audit; defaults reverted |
| `b9c7ee5b4e` | WAL compression, random IDs | 113.54 | Not available | Agent cancelled incomplete exploration before ordered-ID candidate |
| `6a5cd1da1a` | Ordered IDs + WAL compression | 107.18 | 47.35s | Full audit passed, but only 3,033,890 notifications |
| `d11cbd5b6d` | Initial batch-transport build | Not run | Not available | Tests caught missing optional boolean deserialization; fixed using nullable opt-in, not relaxed global parsing |
| `a64b2150b2` | Final, including opt-in delivery batches | **199.30** | **253.13s** | **Full audit passed, 5,656,556 notifications** |

The 47.35-second intermediate result is not the final result: restoring throughput increased notification volume markedly. Cancelled runs have explicit `cancelled.json` records and are not treated as successful latency results. The build failure's Maven log and failure report are retained. Machine-readable summaries are in `notification-throughput-latency-runs.json`.

## Reproduce and evidence

```bash
python3 scripts/test-notifications.py --variant notification --subscribers 100 \
  --stations 10 --duration 60 --min-items 1 --max-items 20 --drain-timeout 600
```

The 600-second limit was never increased. The runner's new WAL default is LZ4; `--notification-wal-compression off` restores the old WAL setting for an explicitly different-runtime comparison. No SKU scope, subscriber count, basket size, opening inventory or teacher-client request logic was reduced.

The final run retains the raw load report, `verification.json`, `metrics-audit.json`, `metrics-query-plan.json`, `environment.json`, `implementation-hashes.json`, source verification, console output, subscriber JSONL logs, database/service/broker text logs and live database evidence. **186 Java tests passed with no failures/errors/skips, including real PostgreSQL tests; 33 Python tests passed.** New tests cover bulk rollback/conflicting identities, UUID layout and legacy migration, flush-before-ACK failure/reconnect behavior, protocol negotiation, batch bounds and frame validation.

Older subscriber logs were losslessly gzip-compressed to make disk space. Each `subscriber-log-archives.json` contains uncompressed SHA-256 hashes checked before replacing original files; `gunzip subscribers/*.jsonl.gz` restores them. Reports and text logs were not deleted. Only verified stopped test PostgreSQL/Kafka temporary data is cleaned. A small stopped build-failure database cleanup occurred during final drain for disk headroom; this is recorded in its cleanup manifest and is another reason not to interpret the timing as a fully controlled experiment.

After final verification passed and all test processes stopped, the final run's PostgreSQL/Kafka temporary data was also removed to restore disk headroom. Its reports, service/database/broker text logs and uncompressed subscriber JSONL logs remain; `temporary-data-cleanup.json` records the cleanup.

Follow-up: before the compact-state experiment, this run's subscriber JSONL files were also losslessly gzip-archived with verified uncompressed hashes in `subscriber-log-archives.json`. Reports and text logs remain. See `compact-notification-state.md` for the later schema migration and its measured tradeoffs.

## Modified/added implementation files

- `notification-service/database/schema.sql`
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`
- `notification-service/src/main/java/edu/cs6510/notification/Protocol.java`
- `notification-service/src/main/java/edu/cs6510/notification/SubscriberHandler.java`
- `subscriber-client/src/main/java/edu/cs6510/subscriber/SubscriberMain.java`
- `subscriber-client/src/main/java/edu/cs6510/subscriber/ReceiptBatch.java` (new)
- `notification-service/src/test/java/edu/cs6510/notification/NotificationStorePostgresTest.java`
- `notification-service/src/test/java/edu/cs6510/notification/AcknowledgementBatchTest.java` (new)
- `notification-service/src/test/java/edu/cs6510/notification/DeliveryBatchTest.java` (new)
- `subscriber-client/src/test/java/edu/cs6510/subscriber/ReceiptBatchTest.java` (new)
- `subscriber-client/src/test/java/edu/cs6510/subscriber/DeliveryBatchTest.java` (new)
- `subscriber-client/src/test/java/edu/cs6510/subscriber/SubscriberFlowControlTest.java`
- `scripts/benchmark_runtime.py`, `scripts/test-notifications.py`, `scripts/compare_fixed_runs.py`
- `scripts/tests/test_benchmark_runtime.py`, `scripts/tests/test_compare_fixed_runs.py`

Checkout business code, teacher load-client code, and the sibling event-driven-server were not modified. Further low-latency work needs to address sustained fan-out capacity (for example, compact pending state or a shared durable sale log with subscriber progress) rather than claiming a longer timeout or a smaller test is an optimization.
