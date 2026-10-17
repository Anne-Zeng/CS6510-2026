# Performance Report / 性能报告

[English](#english) | [中文](#中文) | [Project README](README.md)

## English

### Scope and test conditions

The optimized Service-Based server retains three independently deployable domain services, one centralized PostgreSQL database, the existing HTTP API, and the unmodified load client. Catalog, Checkout and Inventory, and Analytics exchange business data through the shared database. The gateway routes client requests; the domain services do not call each other directly.

Both required workloads were run on October 16, 2026:

| Workload | Stations | Requested duration | Items per transaction |
|---|---:|---:|---:|
| Default | 10 | 60 seconds | 1–20 |
| Stress | 100 | 120 seconds | 1–20 |

Each workload starts with a fresh dedicated database containing 2,000 catalog items and 1,000,000 units of stock per SKU. PostgreSQL uses `READ COMMITTED` isolation and synchronous commits. The recorded environment is macOS on ARM64, 8 logical CPUs, Java 22.0.1 running Java 21-targeted code, and PostgreSQL 15.19. Each JVM uses `-Xms64m -Xmx256m`; the maximum database connection pools are Catalog 8, Checkout 16, and Analytics 8. See the complete [environment record](reports/submission/environment.json).

### Published results

TPS means completed transactions divided by the client's measured wall-clock duration. The wall-clock duration slightly exceeds the requested duration because stations finish transactions already in progress. Analytics draining and database audits happen after the client run; they are not part of the client's TPS measurement.

| Workload and raw report | Wall-clock seconds | Completed transactions | Items scanned | TPS | Audited windows | Last window end |
|---|---:|---:|---:|---:|---:|---:|
| [Default JSON](reports/submission/report-default.json) | 60.111199 | 13,392 | 141,277 | 222.79 | 282 | 141,000 |
| [Stress JSON](reports/submission/report-stress.json) | 120.321107 | 27,691 | 289,996 | 230.14 | 579 | 289,500 |

All measured operations had **zero request errors** in both workloads. Latencies below are per HTTP operation, in milliseconds, rounded to three decimal places; they are not end-to-end basket latencies.

| Workload | Operation | Mean (ms) | P95 (ms) | P99 (ms) |
|---|---|---:|---:|---:|
| Default | Start transaction | 2.893 | 6.674 | 17.375 |
| Default | Scan item | 3.323 | 7.475 | 17.629 |
| Default | Complete transaction | 6.880 | 14.322 | 31.087 |
| Stress | Start transaction | 33.565 | 63.325 | 150.221 |
| Stress | Scan item | 33.492 | 64.958 | 136.887 |
| Stress | Complete transaction | 49.670 | 134.693 | 227.552 |

The [Default audit](reports/submission/correctness-default.json) and [Stress audit](reports/submission/correctness-stress.json) confirm that every accepted scan has exactly one raw outbox record and one matching analysis-log record, no outbox events remain pending, and all expected windows match their persisted input. Rankings also survive an Analytics process restart.

### Comparison with earlier runs

Each of the first three result columns shows **completed transactions / TPS**, with a link to the original JSON. TPS improvements use the unrounded `transactionsPerSecond` values: `(final TPS / earlier TPS − 1) × 100%`.

| Workload | Earlier Service-Based | Historical Pipeline | Final Service-Based | TPS gain over earlier Service-Based | TPS gain over historical Pipeline |
|---|---:|---:|---:|---:|---:|
| Default: 10 stations / 60 s | [5,270 / 87.73](reports/comparison/service-before-default.json) | [8,494 / 141.42](reports/comparison/pipeline-20261001-default.json) | [13,392 / 222.79](reports/submission/report-default.json) | 153.95% | 57.54% |
| Stress: 100 stations / 120 s | [10,851 / 89.00](reports/comparison/service-before-stress.json) | [21,161 / 175.75](reports/comparison/pipeline-20261001-stress.json) | [27,691 / 230.14](reports/submission/report-stress.json) | 158.58% | 30.95% |

The [comparison directory](reports/comparison/) preserves the earlier Service-Based reports, intermediate JDBC and outbox experiments, and Pipeline reports copied from the October 1 assignment run. The final client-generated reports are in [submission](reports/submission/); their measurement values have not been edited.

These are historical comparisons, not a controlled simultaneous experiment. Although the load settings match, the Pipeline runs were recorded on a different date and used different JVM heap settings. Background activity and JVM warm-up may also affect results. Several code changes were combined without a controlled experiment for each individual change, so the full improvement cannot be attributed to the outbox, one SQL statement, or one gateway optimization. The results describe these runs and do not establish that Service-Based architecture is universally faster than Pipeline architecture.

### Implementation changes

1. **Insert new transactions directly.** The application generates the UUID, and JDBC performs a direct `INSERT`, avoiding JPA merge's existence query for an assigned primary key.
2. **Combine scan writes into one database request.** [`CheckoutStore.scan`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/CheckoutStore.java) uses a CTE to lock the transaction, upsert the basket line, update the item count and amount, and append the raw scan to `scan_outbox`. PostgreSQL still executes the necessary reads and writes; this reduces client/database round trips, entity loading, and flush overhead.
3. **Assign analysis sequence numbers in batches.** Checkout writes the outbox atomically without acquiring the global analysis-numbering lock. [`ScanEventStore.promotePending`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/ScanEventStore.java) lets Analytics promote up to 1,000 committed events at a time into contiguous `scan_events.sequence` values. It inserts the analysis log and marks the source events processed in the same transaction. The coordination lock belongs to Analytics batching; checkout scan commits do not wait for that lock.
4. **Combine inventory deduction and completion writes.** Completion first locks the basket and reads its lines. [`CheckoutStore.completeWithInventory`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/CheckoutStore.java) then uses one CTE request to lock inventory in SKU order, conditionally deduct the whole basket, and update the transaction status if the deduction count matches. The business layer checks both the deducted-line count and the single completion update; a failed check rolls back the outer transaction.
5. **Reduce temporary gateway allocations.** [`GatewayServer`](gateway/src/main/java/edu/cs6510/servicebased/gateway/GatewayServer.java) prepares backend root addresses once instead of compiling a trailing-slash regular expression per request. For a known `Content-Length` up to 1 MiB, it reads the exact byte count and verifies completeness, avoiding the generic reader's temporary buffer for small JSON bodies. Empty bodies reuse an empty array. Chunked or larger bodies retain the generic read path; the allocation threshold is not a request-size limit.

### Transactions, ordering, and recovery

Creation, scanning, and completion all use explicit Spring-managed PostgreSQL transactions. A successful scan response means that the basket change and its raw outbox event have committed together. If the event insert fails or silently inserts no row, the transaction rolls back both changes. Synchronous durable commits remain enabled.

Raw `scan_outbox.event_id` values are unique `IDENTITY` identifiers, not contiguous scan positions: rollbacks may leave gaps, so `MAX(event_id)` must not be used as an event count. Analytics acquires its batch transaction lock first, then uses a separate SQL statement with a fresh `READ COMMITTED` snapshot to read committed pending events. Each analysis-log row references a unique `source_event_id`. Log insertion and source marking commit together or roll back together, allowing retry after a failure without losing or duplicating committed events in the analysis log.

The supported deployment uses **one Analytics process writing window results per database**. Its local pipeline builds windows, calculates rankings, and persists results and the checkpoint. The promotion lock coordinates event-number assignment; it does not make the entire window pipeline safe for multiple active Analytics writers.

The window size remains **1,000 scans**, the slide remains **500 scans**, and the first partial window is published at scan 500. The global analysis order is defined by committed Analytics batch order, then raw event ID order within each batch. Scans on the same basket and non-overlapping requests retain their order. Concurrent transactions can allocate IDs and commit in different orders, so this does **not** promise strict commit-time ordering among overlapping scans. This is an explicit input-ordering change: a historical and a new implementation's corresponding window need not contain exactly the same concurrent events.

Rankings are eventually consistent. Checkout can keep accepting durable outbox events while Analytics is offline. After restart, Analytics promotes the backlog and resumes from its persisted window checkpoint. A final tail of fewer than 500 events is also promoted into the contiguous log, but contributes to a new ranking only when a subsequent window boundary is reached. The benchmark waits for zero pending outbox records, matching raw/log counts, and all due windows to be saved before auditing and shutting down.

### Correctness evidence

The published validation evidence records **[19 passing Maven tests](reports/submission/unit-tests.json)** and **[10 passing PostgreSQL acceptance scenarios](reports/submission/integration-checks.json)**.

Gateway tests cover fixed-length UTF-8 bodies, unknown-length chunked Unicode, the large-body fallback, and rejection of truncated uploads before they reach a domain service. Database scenarios cover invalid scans; failed outbox inserts; insufficient-stock rollback; duplicate completion; competing purchases of the last stock; concurrent scans; scan/completion races; and concurrent completion of baskets with opposite SKU order. Recovery testing accepts 1,447 scans while Analytics is offline, injects an analysis-log write failure to verify batch rollback, and then checks exact recovery of the 500, 1,000, and 1,500 boundaries without duplicates.

After each formal workload, the independent audit checks transaction counts and amounts, inventory conservation, both event logs, contiguous analysis positions, one-to-one source mapping and SKU identity, every window's exact ranking, low-stock alerts, and persistence across Analytics restart. A failed run cannot replace the submission reports.

### Reproduce the checks

Install the prerequisites and configure PostgreSQL as described in [README.md](README.md), then run from the repository root:

```bash
# Purpose: enter the project and package the JARs while running Maven tests.
cd service-based-server
./mvnw clean package
# Purpose: verify real database behavior, then run both required workloads using those JARs.
python3 scripts/test-system.py
./scripts/run-benchmarks.sh --skip-build
```

The benchmark compiles the unchanged load client, runs Default with the client's workload defaults, then runs Stress with `--stations=100 --duration=120`. It resets the dedicated `servicebased_acceptance` database's `public` schema before each workload; do not use that database for application data you need to keep. The command above reuses the JARs just packaged; omit `--skip-build` when a new build is needed. The PostgreSQL acceptance suite separately resets `servicebased_acceptance_checks`. Both workloads and audits must pass before the script updates `reports/submission/`. New hardware or background load can produce different measurements.

## 中文

### 范围与测试条件

优化后的 Service-Based 服务器保留三个可独立部署的业务服务、一个集中式 PostgreSQL 数据库、原有 HTTP API，以及未经修改的负载客户端。Catalog、Checkout and Inventory、Analytics 通过共享数据库交换业务数据。网关负责转发客户端请求，业务服务之间不直接调用。

以下两组规定负载均于 2026 年 10 月 16 日运行：

| 负载 | Stations | 配置的运行时间 | 每笔交易商品数 |
|---|---:|---:|---:|
| Default | 10 | 60 秒 | 1–20 |
| Stress | 100 | 120 秒 | 1–20 |

每组测试都从全新的专用数据库开始，包含 2,000 个商品、每 SKU 1,000,000 件库存。PostgreSQL 使用 `READ COMMITTED` 隔离级别并开启同步提交。记录的环境为 ARM64 macOS、8 个逻辑 CPU、Java 22.0.1（运行以 Java 21 为目标编译的代码）和 PostgreSQL 15.19。每个 JVM 使用 `-Xms64m -Xmx256m`；数据库连接池上限为 Catalog 8、Checkout 16、Analytics 8。完整信息见[运行环境记录](reports/submission/environment.json)。

### 最终提交结果

TPS 使用完成交易数除以客户端测得的实际总耗时。实际时间略超过配置的 60／120 秒，因为各站点会完成已经开始的交易。等待分析积压处理完成及数据库审计发生在客户端运行结束之后，不计入客户端 TPS 的测量时间。

| 负载与原始报告 | 实际总耗时（秒） | 完成交易数 | 扫描商品数 | TPS | 审计通过的窗口数 | 最后窗口终点 |
|---|---:|---:|---:|---:|---:|---:|
| [Default JSON](reports/submission/report-default.json) | 60.111199 | 13,392 | 141,277 | 222.79 | 282 | 141,000 |
| [Stress JSON](reports/submission/report-stress.json) | 120.321107 | 27,691 | 289,996 | 230.14 | 579 | 289,500 |

两组负载的所有测量操作均为**零请求错误**。下表延迟以毫秒为单位，保留三位小数；这些是单次 HTTP 操作的延迟，并非整个购物篮的端到端耗时。

| 负载 | 操作 | 平均（ms） | P95（ms） | P99（ms） |
|---|---|---:|---:|---:|
| Default | 创建交易 | 2.893 | 6.674 | 17.375 |
| Default | 扫描商品 | 3.323 | 7.475 | 17.629 |
| Default | 完成交易 | 6.880 | 14.322 | 31.087 |
| Stress | 创建交易 | 33.565 | 63.325 | 150.221 |
| Stress | 扫描商品 | 33.492 | 64.958 | 136.887 |
| Stress | 完成交易 | 49.670 | 134.693 | 227.552 |

[Default 审计](reports/submission/correctness-default.json)和 [Stress 审计](reports/submission/correctness-stress.json)确认：每条已接受扫描恰好对应一条原始 outbox 记录和一条匹配的分析日志，没有待处理的 outbox 事件，所有到期窗口的排名均与持久化输入一致。Analytics 进程重启后，排名也保持不变。

### 与历史运行比较

前三个结果列均为**完成交易数 / TPS**，点击数值可查看原始 JSON。提升百分比使用 JSON 中未四舍五入的 `transactionsPerSecond` 计算：`(最终 TPS / 比较对象 TPS − 1) × 100%`。

| 负载 | 优化前 Service-Based | 历史 Pipeline | 最终 Service-Based | TPS 较优化前提升 | TPS 较历史 Pipeline 提升 |
|---|---:|---:|---:|---:|---:|
| Default：10 stations / 60 秒 | [5,270 / 87.73](reports/comparison/service-before-default.json) | [8,494 / 141.42](reports/comparison/pipeline-20261001-default.json) | [13,392 / 222.79](reports/submission/report-default.json) | 153.95% | 57.54% |
| Stress：100 stations / 120 秒 | [10,851 / 89.00](reports/comparison/service-before-stress.json) | [21,161 / 175.75](reports/comparison/pipeline-20261001-stress.json) | [27,691 / 230.14](reports/submission/report-stress.json) | 158.58% | 30.95% |

[比较目录](reports/comparison/)保留优化前 Service-Based 报告、各轮 JDBC 和 outbox 实验报告，以及从 10 月 1 日原作业运行中复制的 Pipeline 报告。最终客户端原始报告位于[提交目录](reports/submission/)，其中的测量值没有经过修改。

这些是历史运行比较，并非同时进行的严格受控实验。负载配置一致，但 Pipeline 在不同日期运行，且 JVM 堆配置与本次不同；后台活动和 JVM 预热也可能影响结果。多项代码优化合并后运行，没有逐项进行受控实验，因此不能将全部提升归因于 outbox、某条 SQL 或网关的某个小优化。这些结果说明本次实际运行中的变化，不能据此断言所有 Service-Based 架构都比 Pipeline 快。

### 代码优化

1. **创建交易直接 INSERT。** 业务层生成 UUID，JDBC 直接插入，省去 JPA merge 对已分配主键的存在性查询。
2. **扫描合并为一次数据库请求。** [`CheckoutStore.scan`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/CheckoutStore.java) 的 CTE 依次锁定交易、UPSERT 商品明细、更新件数和金额，再向 `scan_outbox` 追加原始扫描事件。数据库内部仍执行必要的查询和写入；减少的是客户端与数据库之间的往返，以及实体加载和 flush 开销。
3. **分析序号改为批量分配。** Checkout 原子写入 outbox，不申请全局分析编号锁。[`ScanEventStore.promotePending`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/ScanEventStore.java) 由 Analytics 每批处理最多 1,000 条已提交事件，生成连续的 `scan_events.sequence`，并在同一事务标记来源事件已处理。协调锁只用于分析批处理，扫描提交无需等待该锁。
4. **合并库存扣减与完成状态写入。** 完成交易先锁购物篮并读取明细，再调用 [`CheckoutStore.completeWithInventory`](shared-persistence/src/main/java/edu/cs6510/servicebased/shared/persistence/CheckoutStore.java)，用一次 CTE 请求按 SKU 顺序锁库存、条件扣减整篮商品，并在扣减数量符合预期后更新交易状态。业务层核对库存扣减行数和单笔完成更新；任一检查失败都让外层事务回滚。
5. **减少网关临时内存分配。** [`GatewayServer`](gateway/src/main/java/edu/cs6510/servicebased/gateway/GatewayServer.java) 启动时预处理后端根地址，不再每请求编译去尾斜线的正则。已知 `Content-Length` 且不超过 1 MiB 时，按准确字节数读取并核对完整性，避免为小 JSON 使用通用读取的大临时缓冲；空正文复用空数组。未知长度分块正文及更大正文保留通用读取路径，预分配阈值不会变成请求大小限制。

### 事务、事件顺序与恢复

创建、扫描和完成结账均使用 Spring 管理的显式 PostgreSQL 事务。扫描成功响应意味着购物篮修改和原始 outbox 事件已经一起提交。事件写入失败或被静默取消而没有插入记录时，事务会回滚两部分修改。同步持久化提交保持开启。

原始 `scan_outbox.event_id` 是唯一的 `IDENTITY` 标识，并非连续扫描位置：回滚后允许出现空洞，因此不能用 `MAX(event_id)` 代替事件数量。Analytics 先取得批处理事务锁，再通过另一条 SQL 的新 `READ COMMITTED` 快照读取已提交的待处理事件。每条分析日志引用唯一的 `source_event_id`。日志插入与来源标记一起提交、一起回滚，失败后可以重试，避免已提交事件在分析日志中丢失或重复。

当前支持的部署方式是：**每个数据库只运行一个写入窗口结果的 Analytics 进程**。其本地 pipeline 构建窗口、计算排名，并持久化结果和检查点。事件整理锁协调序号分配，不代表整个窗口 pipeline 已支持多个同时运行的 Analytics 写入者。

窗口大小仍为 **1,000 次扫描**、步长为 **500 次扫描**，首次在第 500 次扫描处发布部分窗口。全局分析顺序由 Analytics 已提交批次的先后顺序，以及批内原始事件 ID 顺序定义。同一购物篮内的扫描和不重叠请求保留先后关系。并发事务可能先分配 ID、较晚才提交，因此不保证所有重叠扫描严格按提交时刻排序。这是输入排序规则的明确变化：历史实现与新实现的对应窗口不必包含完全相同的并发事件。

排名具有最终一致性。Analytics 停机时，Checkout 可以继续接受并持久化 outbox 事件。恢复后，Analytics 整理积压，并从持久化窗口检查点继续计算。最后不足 500 条的尾部事件也会进入连续分析日志，但需要等到后续窗口边界才产生新排名。正式测试脚本等待 outbox 无待处理事件、原始与连续日志数量一致，以及所有到期窗口已保存后，再审计并关闭服务。

### 正确性证据

已发布的验证证据记录了 **[19 项 Maven 测试通过](reports/submission/unit-tests.json)**和 **[10 组 PostgreSQL 验收通过](reports/submission/integration-checks.json)**。

网关测试涵盖固定长度 UTF-8、未知长度分块 Unicode、大正文回退，以及截断上传不能到达业务服务。数据库场景包括无效扫描、outbox 写入失败、库存不足回滚、重复完成、最后库存竞争、并发扫描、扫描与完成竞争，以及相反 SKU 顺序的购物篮并发完成。恢复测试在 Analytics 停机期间接受 1,447 次扫描，并注入分析日志写入故障验证整批回滚，随后检查第 500、1,000、1,500 个事件边界的精确恢复且没有重复。

每组正式负载结束后，独立审计核对交易数量和金额、库存守恒、两张扫描日志、连续分析位置、来源事件的一一对应与 SKU、每个窗口的精确排名、低库存提醒，以及 Analytics 重启后的持久化结果。失败运行不能覆盖提交报告。

### 复现步骤

先按 [README.md](README.md) 安装依赖并配置 PostgreSQL，再从仓库根目录执行：

```bash
# 功能：进入项目、打包独立 JAR，并运行 Maven 测试。
cd service-based-server
./mvnw clean package
# 功能：验收真实数据库行为，再复用刚生成的 JAR 运行两组规定负载。
python3 scripts/test-system.py
./scripts/run-benchmarks.sh --skip-build
```

正式脚本编译未经修改的负载客户端，先使用客户端默认负载运行 Default，再以 `--stations=100 --duration=120` 运行 Stress。每组负载开始前会重置专用 `servicebased_acceptance` 数据库的 `public` schema，因此不要在该数据库中保存需要保留的应用数据。以上命令复用刚完成打包的 JAR；需要重新构建时省略 `--skip-build`。PostgreSQL 验收套件另外重置 `servicebased_acceptance_checks`。只有两组负载及审计都通过后，脚本才更新 `reports/submission/`。不同硬件和后台负载可能得到不同测量值。
