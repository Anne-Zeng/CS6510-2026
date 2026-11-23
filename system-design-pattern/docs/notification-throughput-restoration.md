# Notification throughput restoration

## Outcome

The predeclared three-run candidate median passes the fixed-work comparison against both the original throughput baseline and the previous low-latency version. The throughput difference from the original baseline is only about 0.1%: this supports **returning to baseline level in this experiment**, not a statistically significant speedup or a guarantee for every run.

Every run has 600 fixed ten-item sales, ten stations, 100 subscribers, 2000 SKUs and the same recovery probes. The independent verifier checks exactly **300,250 notifications**, no missing/unexpected/wrong-quantity/duplicate log entries, zero load errors and zero pending notifications.

| Reference | Runs | Median checkout TPS | Median notification P95 |
|---|---:|---:|---:|
| Original fixed-work baseline | 2 | 97.45 | 52.01s |
| Previous low-latency version | 2 | 93.59 | 16.15s |
| New combined writes + idle backoff | 3 | **97.55** | **15.74s** |

All candidate runs are retained; none is omitted:

| Run suffix | TPS | P95 |
|---|---:|---:|
| 71c246a7fd | 97.55 | 15.74s |
| 9636f9c49c | 101.86 | 15.20s |
| 6a583a972f | 94.48 | 15.88s |

The third run is below the original median. Small sequential local samples cannot establish a statistical performance guarantee. This is the separate fixed-count diagnostic driver, not the original teacher load-client and not the historical 264 TPS event-driven benchmark.

The intermediate combined-write-only run `6dda52e00f` produced 94.50 TPS and P95=14.05s. Idle backoff trades some additional idle-channel wakeup delay for fewer empty database transactions and better checkout resource availability. Load/drain/replay/verification elapsed median is 35.17s, versus 33.30s for the prior low-latency version; that aggregate is not pure delivery latency.

## Changes and invariants

1. `NotificationStore.dispatchBatch` combines sent status and acknowledgements in one owner-filtered `UPDATE ... FROM jsonb_to_recordset`. A notification present in both lists gets one row update instead of two. Already-acknowledged rows are not given a new retry timestamp. Sent-only rows still get retry scheduling; duplicate ACKs preserve their original timestamp.
2. Invalid/foreign ACKs fail the complete transaction. Updates and ownership checks remain atomic. A real PostgreSQL audit trigger regression proves overlapping sent/ACK records are each updated only once. Existing restart, replay, deduplication and rollback checks remain enabled.
3. `SubscriberHandler` backs off empty polling from 200ms up to **1000ms**. Writes or fetched deliveries reset the delay; a nonempty delivery batch continues immediately without sleeping. An idle subscriber can therefore wait up to one second before the next poll, in addition to processing time. This is not a one-second end-to-end SLA.
4. The prior transaction-local custom-plan fix remains enabled. Database dispatch concurrency stays at one; fetch size remains 512; sale-ingestion transactions remain 16. No checkout business logic, durability settings, subscription coverage or original load-client source is changed.

Configuration:

- `NOTIFICATION_COALESCE_DISPATCH_WRITES=true` (false retains the two-write path for comparisons).
- `NOTIFICATION_MAX_IDLE_POLL_MS=1000` (200 disables idle backoff; valid range 200–1000).
- `NOTIFICATION_DISPATCH_CUSTOM_PLAN=true` and `NOTIFICATION_DISPATCH_DB_CONCURRENCY=1` unchanged.

Diagnostics label the combined write as `stateWriteMs`, not ACK-only time. Older `sentMs`/`ackMs` fields remain available for the legacy write path. The harness's separate legacy sent/ACK EXPLAIN probes do not describe the new combined statement.

## Validation and artifacts

**165 Java tests passed with no skips**, including real PostgreSQL tests; **29 Python tests passed**. Each end-to-end run verifies scanning without sale, idempotent completion, offline delivery, notification-service restart and outbox replay.

Machine-readable comparisons:

- `docs/notification-restoration-vs-baseline.json`
- `docs/notification-restoration-vs-low-latency.json`

Both comparisons report `passed=true`; they require matching fixed plans/counts/environment, correct delivery, non-decreasing median TPS and lower median P95. Baseline source hashes and unchanged teacher-client source hashes remain in the run environment files. The fixed plan hash is `ca1de290bf18f4f45f01418f3cf9b381556c3e22fda68860b3d73d9bf7bd4278`.

```bash
python3 scripts/test-notifications.py --subscribers 100 --stations 10 \
  --fixed-sales 600 --fixed-items 10 --drain-timeout 600 --diagnostics
```

Reports, service logs and subscriber logs remain under `target/notification-test-<suffix>`. Stopped temporary PostgreSQL/Kafka data may be removed to reclaim disk; it is not needed to inspect these results. The original event-driven-server and teacher load-client are untouched.
