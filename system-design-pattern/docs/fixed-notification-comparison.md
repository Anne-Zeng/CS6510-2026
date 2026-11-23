# Fixed-notification experiment

## Result

The ACK bottleneck is identified and notification latency improves, but the strict combined performance acceptance **does not pass**: median checkout TPS is still 3.96% below baseline. No claim of unchanged throughput or statistical significance is made.

Every run uses 600 fixed sales, ten distinct deterministic SKUs per sale, ten stations, 100 subscribers, 2000 catalog SKUs, and two identical recovery sales. Every verified run has exactly **300,250 notifications**, zero missing/unexpected/wrong-quantity/duplicate log entries, zero load errors, and zero pending deliveries after drain.

Plan SHA-256: `ca1de290bf18f4f45f01418f3cf9b381556c3e22fda68860b3d73d9bf7bd4278`.

| Run suffix | Configuration | Checkout TPS | Notification P95 seconds |
|---|---|---:|---:|
| a429d216e2 | baseline, cached planning, concurrency 4 | 98.77 | 51.94 |
| d7bb7fb6b6 | baseline repeat, slow cached-plan capture | 96.12 | 52.09 |
| 36a85c1e11 | custom planning, concurrency 4 | 81.97 | 16.23 |
| 321908135b | write-only custom planning, concurrency 2 | 87.10 | 16.55 |
| dfbf50b21e | write-only custom planning, concurrency 1 | 94.45 | 16.50 |
| 6685a977ba | final: concurrency 1, SKU set membership | 92.89 | 15.87 |
| 6533c80dda | final repeat, same configuration | 94.28 | 16.44 |

Two baseline / two final run medians:

- Notification P95: **52.01s → 16.15s**, down **68.95%**.
- Checkout TPS: **97.45 → 93.59**, down **3.96%**.
- Notification P50: 15.73s → 10.11s.
- Load, drain and recovery/verification elapsed: 68.74s → 33.30s (not pure load duration).

The fixed-count diagnostic driver is separate from the teacher's original load-client, which remains unchanged. These TPS values should not be compared directly to historical teacher-client benchmarks. Runs are sequential on one laptop; fixed work does not fix event arrival timing or eliminate thermal/OS variance. Intermediate configurations are included rather than hidden.

## Bottleneck evidence and changes

In `target/notification-test-d7bb7fb6b6/services/notification.log`, `NOTIFICATION_SLOW_ACK_PLAN` captures the actual prepared statement on the slow JDBC connection. It reports `genericPlans=68`, `customPlans=5`, a `Seq Scan`, and a nested-loop semi-join with estimates appropriate to a tiny table. Reusing that plan on the growing delivery table makes ACK operations expensive. A fresh EXPLAIN in another connection is not sufficient to see this cached plan.

Baseline busy-cycle ACK SQL P95 is 193.99–367.90ms, versus 5.54–9.90ms for fetching deliveries. Gate wait P95 is 9.40–9.79s. Final ACK SQL P95 is 9.88–10.09ms; gate wait P95 is 0.99–1.11s.

- `NotificationStore`: use transaction-local `force_custom_plan` for writing dispatch cycles only, with timing and diagnostic cached-plan capture. Empty polling does not change planning mode. Diagnostics use EXPLAIN without executing the diagnostic UPDATE and a savepoint for failure isolation.
- `DispatchDatabase`: final default concurrency is one, still configurable. It covers database work through commit, not WebSocket sends. This is laptop-specific resource tuning, not a universal optimum.
- `SubscriberMain`: cache subscribed SKUs in a set instead of repeated linear membership searches; no change to subscriptions, logging, ACKs or delivery semantics.
- Harness: deterministic fixed baskets and stations, independently checked notification totals, saved manifest/hash and environment, plus comparison rejecting unequal workloads or failed correctness.

Batch size stays 512; ingestion transactions stay at 16 sales; durability, owner validation, retries, replay deduplication, and the original checkout business logic remain intact.

## Evidence and reproduction

Each run directory is `target/notification-test-<suffix>`. Reports/logs remain; stopped temporary database/Kafka data from the first four runs were removed to reclaim space. The comparison artifact is `docs/fixed-notification-comparison.json`; its `passed=false` reflects only the TPS criterion, not a correctness failure.

```bash
python3 scripts/test-notifications.py --subscribers 100 --stations 10 \
  --fixed-sales 600 --fixed-items 10 --drain-timeout 600 --diagnostics

python3 scripts/compare_fixed_runs.py \
  --baseline target/notification-test-a429d216e2 target/notification-test-d7bb7fb6b6 \
  --candidate target/notification-test-6685a977ba target/notification-test-6533c80dda \
  --output docs/fixed-notification-comparison.json
```

Final validation: **160 Java tests passed, zero skips; 28 Python tests passed**. The real PostgreSQL tests cover transaction-local planning, transactional ACK rollback, ownership, bounds and retry persistence; end-to-end tests cover reconnect, service restart and outbox replay.
