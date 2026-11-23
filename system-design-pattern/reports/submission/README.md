# Notification Service — Submission Report

## Video

[Watch the Notification Service demonstration on Panopto](https://northeastern.hosted.panopto.com/Panopto/Pages/Viewer.aspx?id=d9df3ffb-0ff4-4167-bb20-b50b00a1ea41). The URL was supplied by the author; instructor playback permissions have not been independently verified.

## Design and requirements

This implementation extends the event-driven supermarket system. The original load-client sends HTTP requests to the Gateway, which uses Kafka request–reply commands to reach Checkout. Successful checkout atomically records transaction completion and a `SaleCompleted` event in a PostgreSQL transactional outbox. A background publisher sends this event to the Kafka sales topic. Scanning alone does not create a completed-sale notification.

The Notification Service supports up to 100 registered subscribers, each subscribing to one or more SKUs. It persists subscriptions, consumes sales through a dedicated consumer group, deduplicates events in a durable inbox, and matches sold SKUs to subscriptions eligible at the sale completion time. Each matching transaction/subscriber/SKU has a delivery record carrying the sold quantity. Shared immutable sale-item payloads avoid duplicating the full payload for every recipient.

A configurable subscriber application runs one thread and one WebSocket connection per subscriber. All connections use `/notifications`, but each connection is bound to its own subscriber identity and receives only its pending notifications. The client logs receipts, flushes its buffer, and sends acknowledgements. Unacknowledged notifications remain retryable; replayed notification IDs are deduplicated in client logs. This is at-least-once delivery with deduplication, not exactly-once network delivery. Buffered log flush is not an explicit disk fsync or a guarantee against arbitrary machine power loss.

Key implementation files, relative to the project root:

- `gateway/src/main/java/edu/cs6510/servicebased/gateway/GatewayServer.java`
- `checkout-service/src/main/java/edu/cs6510/servicebased/checkout/persistence/SagaStore.java`
- `checkout-service/src/main/java/edu/cs6510/servicebased/checkout/SaleOutboxPublisher.java`
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`
- `notification-service/src/main/java/edu/cs6510/notification/SalesEvents.java`
- `notification-service/src/main/java/edu/cs6510/notification/SubscriberHandler.java`
- `subscriber-client/src/main/java/edu/cs6510/subscriber/SubscriberMain.java`
- `scripts/verify_notifications.py`

## Completed benchmark results

Both runs used the original load-client, baskets of 1–20 scanned items, and 100 notification subscribers. Default means 10 checkout stations for 60 seconds; stress means 100 stations for 120 seconds. It does not mean the harness's no-argument defaults.

| Metric | Default | Stress |
| --- | ---: | ---: |
| Load-client completed transactions | 5,330 | 24,955 |
| Load wall time (seconds) | 60.078516333 | 120.568560833 |
| Transactions per second | 88.71723746400725 | 206.9776716881051 |
| Scanned items | 56,391 | 263,832 |
| HTTP operation errors | 0 | 0 |
| Completed sales including functional probes | 5,332 | 24,957 |
| Expected notifications | 2,524,798 | 11,785,116 |
| Received notifications | 2,524,798 | 11,785,116 |
| Missing / unexpected / wrong quantity / duplicate log entries | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Notification P50 (seconds) | 147.55280 | 293.60491 |
| Notification P95 (seconds) | 238.07526 | 431.19620 |
| Notification P99 (seconds) | 242.14769 | 461.39524 |
| Complete HTTP request P95 (milliseconds) | 42.506417 | 58.202334 |
| Final pending notifications | 0 | 0 |
| Load / notification / metrics audit passed | true / true / true | true / true / true |
| Recorded load/drain/replay/validation interval (seconds) | 352.54 | 711.13 |
| Power source | AC Power | Battery Power |
| Low-power mode | off | off |

The two additional completed sales are functional-test transactions, not duplicate load-client sales. The notification latency is `receivedAt - completedAt`, including queueing across the full event-to-client path. It is distinct from the Complete HTTP request latency. The recorded elapsed interval includes replay and verification work, not only backlog draining.

## Correctness validation

The verifier independently joins completed Checkout transactions and quantities with persisted subscriptions and subscription times. It compares expected `(transactionId, subscriberId, SKU)` records against subscriber JSONL logs, checking missing, unexpected, duplicate records and quantity mismatches. Metrics are independently audited against database state.

Both completed runs passed scan-without-sale, repeated-completion, offline-delivery, Notification service restart, outbox replay, and original-load-client checks. The final pending count was zero with 100 connected subscribers. These are results of these particular runs, not an unlimited-scale guarantee.

## Environment and limitations

All components ran on one Mac. Five separate PostgreSQL instances still shared the machine's CPU, memory and storage with Kafka, Java services, load-client and subscriber-client. The catalog contained 2,000 SKUs, seeded with 1,000,000 units per SKU to avoid stock exhaustion during the benchmark. Fifty subscribers followed all supplied SKUs; fifty followed one SKU each, creating a deliberately heavy notification fan-out.

JVM options were `-Xms64m -Xmx256m -XX:TieredStopAtLevel=1`. Delivery batch size was 512, ingestion batch size 16, dispatch database concurrency 1, payload cache size 0 and ACK-drain-at-gate false. Additional database and application settings are preserved in `environment.json`.

Both runs passed the harness performance-safety checks, but default was on AC power and stress on battery. Captured load-client source hashes match across the runs; captured application source hashes do not all match. Their per-run hashes and differing paths are preserved in `environment.json`. These runs are individual workload measurements, not a controlled scaling experiment or proof of an optimization speedup. No claim is made that the source differences are behaviorally irrelevant without a historical-content comparison.

The runs demonstrate correct eventual delivery, but not low-latency delivery under heavy load. Default P95 was approximately 238 seconds and stress P95 approximately 431 seconds. High fan-out and shared resources are plausible contributors; identifying the dominant bottleneck requires profiling. The assignment specifies performance measurement but gives no numerical notification latency target. Earlier disk-exhausted runs are not presented as passed tests. No AWS or Explorer deployment is claimed.

## Reproduction

Run from the project root with adequate free disk space and no conflicting test processes. Space must be budgeted for millions of delivery records and receipt logs; the startup minimum is not a full-run capacity guarantee.

```bash
caffeinate -i -s env \
  NOTIFICATION_ACK_DRAIN_AT_GATE=false \
  NOTIFICATION_PAYLOAD_CACHE_SIZE=0 \
  python3 scripts/test-notifications.py \
  --variant notification --subscribers 100 \
  --stations 10 --duration 60 --min-items 1 --max-items 20 \
  --database-servers separate --drain-timeout 600
```

For stress, change only `--stations 100 --duration 120`. Wait for final notification validation, not just the load-client throughput output. The 600-second limit applies to relevant wait phases, not to total program runtime.

## Evidence

- `report-default.json` and `report-stress.json`: byte-for-byte original client JSON outputs.
- `correctness-default.json` and `correctness-stress.json`: byte-for-byte completed harness verification outputs.
- `environment.json`: per-run captured settings and source hashes, with source-hash differences identified.
- `validation.json`: automated consistency checks, SHA-256 evidence hashes, run identifiers and retained diagnostic snapshots.

Local raw runs are `target/notification-test-a736b26880` (default) and `target/notification-test-15b1c6775d` (stress). Raw logs and database data are intentionally not committed. Subscriber credentials, tokens, video binaries and private keys are excluded from this evidence submission.
