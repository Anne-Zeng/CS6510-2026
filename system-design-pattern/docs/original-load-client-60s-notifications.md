# Original load-client: 60-second notification test

## Result: workload passed, notification drain timed out

Run: `target/notification-test-198dcda69a`.

```bash
python3 scripts/test-notifications.py --variant notification \
  --subscribers 100 --stations 10 --duration 60 \
  --min-items 1 --max-items 20 --drain-timeout 600
```

The original teacher load-client was compiled from the unchanged sibling `load-client/src`; the fixed-count diagnostic driver was **not** used. All nine Java source files plus `build.sh` and `run.sh` match the previously verified upstream revision `0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06` byte-for-byte. No business code or client code was modified for this run.

Before running, stopped test PostgreSQL/Kafka data was removed, releasing about 6.22 GiB and leaving about 10.31 GiB free. Other files in the affected runs were checked for continued existence and unchanged size; reports, service logs and subscriber logs were retained. The existing system PostgreSQL service was not touched.

## Configuration

- Ten stations, 60 seconds requested, baskets of 1–20 items, 100 independent subscribers.
- 2000 catalog SKUs, 1,000,000 units of opening inventory per SKU.
- Same subscription policy: even-index subscribers follow all SKUs, odd-index subscribers follow one SKU.
- Five isolated PostgreSQL instances, durable settings enabled, 32MB shared buffers each.
- Notification dispatch concurrency 1, fetch batch 512, sale ingestion batch 16, custom write plans and coalesced state writes enabled, idle polling capped at 1000ms.
- Additional stage diagnostics disabled. Normal harness health/metrics polling remains active.

## Checkout throughput

| Metric | Result |
|---|---:|
| Actual workload duration | 60.062147125s |
| Completed load transactions | 6,956 |
| Checkout TPS | **115.8134** |
| Successful scans | 72,922 |
| Items/second | 1,214.1091 |
| Start / scan / complete errors | **0 / 0 / 0** |

These TPS values are from the original client's report and exclude notification drain/replay time. Do not directly compare them with TPS from the separate fixed-count diagnostic driver.

## Notification outcome

The 600-second post-load drain wait expired with `Timed out waiting for drained`. The overall run failed; no successful independent full-delivery verification or final outbox-replay check was reached.

Last saved metrics snapshot at timeout:

- Expected sales: **6,958**, including two recovery-probe sales in addition to the 6,956 load transactions.
- Sales recorded by notification service: **6,646**; **312** not yet recorded there.
- Generated notification rows: **3,110,225**; **307,708** not yet acknowledged.
- Connected subscribers: **100**.

After shutdown, the retained subscriber logs contain **2,838,533 unique notification records**, zero duplicate IDs within each subscriber log and zero malformed lines. This is log analysis only, not a routing/quantity/full-delivery audit. Some records arrive after the last metrics sample while shutdown proceeds, so these totals need not match that sample exactly.

**Partial, received-only latency** measured from sale completion to the subscriber's received timestamp:

| Percentile | Seconds |
|---|---:|
| P50 | 119.98 |
| P95 | **501.66** |
| P99 | 586.77 |

Unreceived notifications and sales not yet recorded by the notification service are excluded. These are censored, received-only statistics, **not valid full-population completion percentiles**. The complete run's latency cannot be reported as a successful result. The long workload clearly does not preserve the low latency seen in small fixed-count tests.

The evidence shows both unprocessed sales and an unacknowledged delivery backlog; it does not by itself identify the precise bottleneck. This run used no extra stage instrumentation and did not change timeout, subscription coverage or durability to obtain a pass.

## Preserved evidence

Under the run directory:

- `load-report/report-20261117-201223.json`: original client output.
- `throughput.json`, `workload-validation.json`, `environment.json`.
- `failure.json`, `drain-status.json`, `console-output.log`.
- `partial-delivery-summary.json`: received-only statistics with explicit incomplete/audit-not-run status.
- `load-client-source-verification.json`: upstream revision and source hashes.
- `subscribers/*.jsonl`, service logs, and this run's stopped database/Kafka data for investigation.

All test services were shut down by the harness. This failed run's data is preserved; the initial cleanup only removed prior stopped test data.

Follow-up: after read-only database diagnosis, this run's stopped PostgreSQL/Kafka data directories were removed to make space for the next test. All 355 other files, including reports, logs, table statistics and query plans, remain. See `notification-metrics-bottleneck.md` for the subsequent change and retest outcome.
