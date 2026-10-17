# Self Checkout — Service Based Architecture

Repository: [Anne-Zeng/CS6510-2026](https://github.com/Anne-Zeng/CS6510-2026)

Assignment implementation: [`service-based-server/`](README.md)

Optimization details and comparison limits: [PERFORMANCE.md](PERFORMANCE.md).

## Defined domain services

1. **Catalog Service** — returns the product catalog, including SKU, name, and price.
2. **Checkout and Inventory Service** — creates transactions, scans products, returns receipts, completes checkout, updates inventory atomically, and reports low stock.
3. **Analytics Service** — processes committed scan events from the shared database, persists sliding-window Top 10 rankings, and serves the latest published ranking.

Each domain service is an independently deployable Spring Boot JAR with its own JVM and HTTP port. All three use one centralized PostgreSQL database. They do not call one another directly. Checkout persists each raw scan event in `scan_outbox` in the same database transaction as the basket update. Analytics independently promotes committed outbox events into a contiguous analysis log and recovers from durable window checkpoints.

A separate JDK HTTP gateway provides one entry point for the original load client. It is routing infrastructure rather than an additional domain service. The `shared-persistence` JAR distributes common entities, repositories, database access code, and API contracts; it is a library rather than a running service.

Checkout's frequently executed writes use the shared JDBC [`CheckoutStore`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/CheckoutStore.java) to reduce database round trips and JPA entity/flush overhead. A scan locks the transaction, upserts its basket line, updates its totals and inserts the durable outbox event in one SQL request. All checkout mutations retain explicit Spring-managed PostgreSQL transactions with synchronous durable commits. After locking the transaction and reading its basket, `completeWithInventory()` locks inventory rows in SKU order and combines the set-based stock deduction and completion-state update in one CTE request. The service verifies that the number of debited inventory rows equals the basket's line count and that exactly one transaction was marked completed. Insufficient stock or a missing state update causes the entire transaction to roll back.

The gateway caches normalized backend root URLs at startup, avoiding regular-expression work per request. Known-length request bodies up to 1 MiB are read with exact byte sizing and completeness checks; larger or chunked bodies retain the general read path. This threshold limits preallocation rather than accepted request size. Existing HTTP methods, encoded paths, headers, body bytes and response statuses remain covered by gateway tests.

Checkout scans do not acquire a global analysis-sequence lock. Analytics processes up to 1,000 committed outbox events per batch: canonical event insertion and marking each source as processed commit atomically. A unique source-event reference links the two logs. Raw identity IDs may have rollback gaps; canonical analysis sequence numbers are contiguous. Analysis order follows committed promotion batches and raw-ID order within each batch, rather than promising exact commit-time order across concurrent scans. Basket-local scan order and the order of non-overlapping requests are preserved. Ranking remains eventually consistent and exactly replayable from the canonical log. The three independently deployable domain services continue to communicate through the shared database.

## Required load-client JSON files

| Workload | Stations | Requested duration | Original client report |
|---|---:|---:|---|
| Default parameters | 10 | 60 seconds | [report-default.json](reports/submission/report-default.json) |
| Stress: `--stations=100 --duration=120` | 100 | 120 seconds | [report-stress.json](reports/submission/report-stress.json) |

Additional evidence: [Default correctness audit](reports/submission/correctness-default.json), [Stress correctness audit](reports/submission/correctness-stress.json), and [runtime environment](reports/submission/environment.json).

## Verified results

Measured locally on October 16, 2026, using the original load-client source:

| Workload | Completed transactions | Transactions/sec | Request errors | Completion P95 / P99 |
|---|---:|---:|---:|---|
| Default | 13,392 | 222.79 | 0 | 14.32 / 31.09 ms |
| Stress | 27,691 | 230.14 | 0 | 134.69 / 227.55 ms |

Both database audits passed, including inventory conservation, basket quantities/prices,
contiguous scan events, low-stock alerts, and exact rankings for every due window
(282 Default windows from 141,277 scans and 579 Stress windows from 289,996 scans).
Both outboxes were fully processed, with zero source-event or SKU mismatches.
Analytics rankings were preserved across
a service restart in each workload. Requested durations were 60 and 120 seconds;
wall-clock durations were 60.111 and 120.321 seconds while in-flight transactions finished.

All [19 Maven tests](reports/submission/unit-tests.json) and
[10 real PostgreSQL integration scenarios](reports/submission/integration-checks.json) passed,
covering failed-write rollback, concurrent checkout, exact totals under concurrent scans,
the scan-versus-completion race and offline Analytics recovery.
Failure injection also verifies atomic rollback of raw outbox writes and canonical-log
promotion, then checks source-event links, matching SKUs and replay after restart.

The archived pipeline reports and these service-based reports were measured at different
times. They support a historical comparison of those particular runs. Higher throughput
in a later run does not establish that service-based architecture is generally faster;
a controlled architecture comparison requires rerunning both implementations under the
same environment and configuration, with repeated measurements.

## Reproduce

Prerequisites: JDK 21+, PostgreSQL 15+ with `psql`, and Python 3. Configure the database environment as described in the [README](README.md), then run from `service-based-server`:

```bash
# 功能：构建全部独立 JAR 并运行 Maven 测试。
./mvnw clean package

# 功能：验收真实路由、并发结账、事务回滚和 Analytics 重启恢复。
python3 scripts/test-system.py

# 功能：运行要求的两组负载，并分别审计数据库状态。
./scripts/run-benchmarks.sh --skip-build
```

The correctness suite resets only its dedicated `servicebased_acceptance_checks` schema. The benchmark resets only its dedicated `servicebased_acceptance` schema before each workload, using identical fixtures: 2,000 products with 1,000,000 units per SKU. It waits for all outbox events to be promoted and every due window to persist before restart checks and independent database audits. The submission directory is updated only after both workloads and their audits succeed. Analytics uses windows of 1,000 scans with a slide of 500 and an initial partial window at 500; an incomplete final slide is promoted into the durable canonical log without publishing an extra partial window.

## 可复制到 Canvas 的简短说明

> My system defines three independently deployable domain services: Catalog Service, Checkout and Inventory Service, and Analytics Service. All services share one centralized PostgreSQL database and do not communicate directly. Checkout atomically records scan events in a database outbox with basket updates; Analytics independently promotes committed events into a replayable analysis log and persists rankings and recovery checkpoints. A separate HTTP gateway exposes one entry point for the unchanged load client. The required Default and Stress JSON reports are located in `service-based-server/reports/submission/`.

提交前确认上方两份 JSON 和补充验证文件确实存在并检查运行结果，然后将本次作业源码、说明和报告提交到上述仓库。仅在本地生成文件不会自动更新 GitHub；Canvas 应提交可访问的仓库 URL。
