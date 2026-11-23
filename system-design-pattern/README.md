# System Design Pattern — Notification Service

## Assignment submission

- [Video demonstration (Panopto)](https://northeastern.hosted.panopto.com/Panopto/Pages/Viewer.aspx?id=d9df3ffb-0ff4-4167-bb20-b50b00a1ea41)
- [Final default and stress report, correctness evidence, and reproduction instructions](reports/submission/README.md)
- Both completed runs verified all expected notifications with zero missing, unexpected, wrong-quantity or duplicate log entries. Default: 5,330 transactions, 88.72 TPS, notification P95 238.08 seconds. Stress: 24,955 transactions, 206.98 TPS, notification P95 431.20 seconds.
- These results demonstrate eventual delivery correctness, not low latency under heavy load. Power conditions and captured application source hashes differ between runs; see the final report. Earlier experiments below are historical context, not the final submission results.
- Confirm that the instructor can play the linked video before submitting its URL.

基于同级 event-driven-server 的当前本地工作副本实现；原目录未修改。
来源仓库 HEAD：e024f8687cbd3c817eab310a98d77d990feaef6a。
复制时包含原目录已有的未提交修改，因此不等同于该 commit 的纯净快照。

## 实现范围

- 最多 100 个注册订阅者；每人订阅一个或多个 SKU。
- 只有成功成交才产生 SaleCompleted；原来的扫描分析链路保持不变。
- Checkout 交易完成和 sale_outbox 写入在同一条原子 SQL 中提交。
- 后台发布成交事件到 Kafka sales topic，收到 broker ACK 后标记已发布。
- 独立 notification-sales 消费组，Inbox 去重和生成通知在同一数据库事务中提交。
- 每个订阅者一条 WebSocket 连接；单一客户端程序启动可配置数量的订阅线程。
- 通知按交易、订阅者、SKU 去重，数量保留在 quantity 字段中。
- 数据库保留未确认通知；客户端 ACK、断线重连、服务重启后重试。
- 客户端逐用户 JSONL 日志，复用日志中的 notificationId 去重。
- 未实现本次作业的 Rate Limiter。

## 架构

### 接电复测：2026-11-18

用户接电后再次串行执行固定量关闭/开启、原始客户端关闭/开启四轮测试。
全部连续监测为 AC Power、低功耗模式 0；防休眠、时钟和正确性检查全部通过。
两组分别核对服务器和老师客户端源码哈希相同，记录的配置差异仅为
`NOTIFICATION_ACK_DRAIN_AT_GATE`。本轮未修改业务代码，也未开启默认优化。

| 测试 | 开关 | TPS | 通知 P95（秒） | 通知数 |
| --- | --- | ---: | ---: | ---: |
| 固定 2000 笔销售、每篮 10 件 | 关闭 | 117.15 | 14.93 | 1,000,600 |
| 同一固定量 | 开启 | 120.09 | 14.65 | 1,000,600 |
| 原始客户端 10 站点 / 60 秒 / 每篮 1–20 件 | 关闭 | 116.65 | 66.76 | 3,298,431 |
| 同一原始客户端参数 | 开启 | 147.46 | 84.53 | 4,136,298 |

四轮均为 100 订阅者，操作错误、缺失、多余、错误数量和重复日志均为零，恢复检查与独立计数器审计通过。
原始客户端开启后 TPS 增加约 26.4%，但通知数增加约 25.4%，P95 增加约 26.6%；
**仍未同时降低 P95 并恢复 TPS，默认开关继续关闭**。
固定量的小幅收益不能直接外推到 60 秒高负载；每种情况仅一次，不能视为稳定性能或统计显著性证明。
没有将本轮与电池数据混比，也没有宣称达到历史事件服务约 264 TPS 的参考值。

汇总目录：`target/ac-comparison-20261118-155451`，包含 `summary.json`、
`fixed-comparison.json`、`original-comparison.json`、源码核对和 203 项 Java / 44 项 Python 测试结果。
四个原始证据目录分别为 `notification-test-d68d7b3083`、`notification-test-936f92fcd1`、
`notification-test-92bfeca5ae`、`notification-test-c6765f4b1f`。
已停止的临时数据库/Kafka 数据已清理；报告、服务日志和经 SHA-256 校验的无损压缩订阅日志保留。

### ACK 排队合并优化

`SubscriberHandler` 以前在等待发送侧数据库许可之前就取走 ACK 队列；等待期间收到的 ACK
只能留到下一轮，使部分通知先更新发送状态、再单独更新 ACK。候选优化开启时通过 supplier 把 ACK
收集移到 `DispatchDatabase` 取得许可之后、调用原有事务之前，尽可能一次提交发送计数和 ACK。
没有增加人为等待、超时或批次大小，也没有把 ACK 当作已经提交；数据库失败时原有事务回滚，
未确认通知仍可重试。等待许可被中断时不会提前取走 ACK，异常路径仍释放许可。
订阅者校验、512 条发送批次、1024 条 ACK 队列、数据库并发 1 和原始客户端均未改变。

实验开关 `NOTIFICATION_ACK_DRAIN_AT_GATE=true` 启用优化，**最终默认 false**，保留原有提前收集方式。
原因是重新控制供电后的固定量对照没有通过 TPS 门槛，不能默认启用可能降低吞吐的实验方案。
四轮防休眠固定量实验按 baseline / candidate / candidate / baseline 顺序运行，
每轮 2000 笔销售、每篮 10 件、10 站点、100 订阅者，恰好 1,000,600 条通知，
全部正确性、恢复检查和时钟检查通过；事后发现前两轮采样为接电、后两轮为电池，
因此这组混合供电的中位数不作为严格性能通过结论。以下是独立固定量驱动，不是老师原始 load-client。

| 模式 | 运行后缀 | TPS | P95（秒） |
| --- | --- | ---: | ---: |
| 提前收集 | 90de2171fe | 131.47 | 12.27 |
| 取得许可后收集 | 9b8e46f250 | 129.53 | 12.05 |
| 取得许可后收集 | 8378a3d368 | 130.26 | 13.21 |
| 提前收集 | dd34b924c2 | 105.15 | 15.81 |
| 提前收集中位数 | 两轮 | 118.31 | 14.04 |
| 取得许可后收集中位数 | 两轮 | 129.90 | 12.63 |

状态物理更新次数中位数由 1,354,442 降至 1,016,418（约 -25%），
累计 WAL 由 747,497,181 降至 629,147,880 字节（约 -16%）。
但投递索引块读取中位数由 396,914 增至 502,825.5，不能宣称所有 I/O 都下降。
每组仅两次，基线存在明显波动且混用了供电状态；旧比较文件的算术门槛通过不等于受控实验通过。
见 `gate-comparison-review.json` 的撤回说明。重新执行比较脚本时，现在会拒绝已知的混合供电结果。
完整结果：`target/notification-test-8378a3d368/gate-comparison.json` 和 `gate-io-comparison.json`。
本轮修改 `DispatchDatabase.java`、`SubscriberHandler.java`、`application.properties`、
`DispatchDatabaseTest.java`、`scripts/test-notifications.py` 和本说明；没有新增数据库迁移。
基准程序还增加了每 2 秒采样的电源/低功耗模式监测，`clock-safety.json` 保存观察到的状态；
切换供电、切换低功耗模式或无法读取状态时标记 `performance_valid=false`。
`compare_fixed_runs.py` 拒绝不同供电条件的比较，原始报告和事后复核说明都保留。

重新监测供电后的固定量对照（均全程电池供电、低功耗模式 0、时钟检查通过）：
`736122dc97`（关闭）→ `5af56ce6c1`（开启），仍是每轮 1,000,600 条通知。
TPS **132.95 → 107.21**，P95 **13.98 → 11.46 秒**，两轮正确性均通过，
但联合性能门槛失败，见 `target/notification-test-5af56ce6c1/power-controlled-comparison.json`。
这些结果不能与历史接电条件的约 264 TPS 直接归因比较。

之后使用老师原始 load-client、实验开关显式开启，按 10 站点 / 60 秒 / 每篮 1–20 件 /
100 订阅者重测：`target/notification-test-f32dade03f`。
9,570 笔负载交易，**159.34 TPS**；通知 P50 **44.50 秒**、P95 **75.26 秒**、P99 **77.01 秒**。
4,537,889 条通知全部匹配，缺失、多余、错误数量、重复日志和负载操作错误均为零；
恢复/重放检查、独立计数器审计通过。防休眠有效，时钟偏差约 -0.008 秒，连续采样记录为
电池供电、低功耗模式 0，`performance_valid=true`，但不是历史接电性能基线的同环境比较。
较早的 `e5c106fb9b` 原始客户端运行也保留，不将不同成交数量的延迟直接作优化归因。

最终结论：发现并消除了候选路径中的一部分重复状态写入，但**没有实现降低 P95 同时恢复 TPS**，
不能把固定量 TPS 回退的候选路径默认启用。默认仍为提前收集 ACK；若要复现实验，显式设置
`NOTIFICATION_ACK_DRAIN_AT_GATE=true`。203 项 Java 测试和 44 项 Python 测试通过。
本轮额外修改 `scripts/benchmark_clock.py`、`scripts/compare_fixed_runs.py` 及对应测试，
防止供电条件混用导致错误的性能结论。所有报告、失败门槛结果和复核说明均保留。

### 投递状态 I/O 优化与回退实验

最终采用原生 `uuid` 存储 `notification_deliveries.notification_id`，数据库字段由文本形式
改为 16 字节 UUID；WebSocket、日志和 ACK 的字符串 ID 保持不变。批量发送/ACK SQL 的
JSON ID 转换为 UUID 后匹配主键，订阅者校验、幂等、重试期限和事务提交语义不变。
仍使用 `(subscriber_id,next_attempt_at,notification_id)` 的未确认通知部分索引；
发送批次仍为 512、发送侧数据库并发仍为 1、内容缓存仍默认关闭。

迁移必须停服务、备份后执行当前 schema，再启动新版本；类型转换可能重写表及索引，
需要预留空间，不能当作在线迁移。合法且规范的历史 UUIDv4/v7 字符串保持原 ID，
ACK 时间、重试时间、发送次数和计数器保留。非法或非规范 ID 会让整个迁移回滚，
不会静默改写客户端已经记录的 ID。现有用户数据库未被自动迁移，实验只使用隔离测试库。

本轮固定量均为 2000 笔销售、每篮 10 件、10 个站点、100 个订阅者，
精确产生 1,000,600 条通知；所有运行的独立正确性检查通过。
这些是顺序单次实验，不构成统计显著性证明，固定量驱动不是老师原始客户端。

| 运行后缀 | 策略 | TPS | 通知 P95（秒） | 决策 |
| --- | --- | ---: | ---: | --- |
| a9ddca0e14 | 原文本 ID / 原重试索引 | 112.87 | 13.07 | 本轮基线 |
| 2db9eacab5 | 稳定 ID 索引 / fillfactor 70 | 101.11 | 13.19 | 回退 |
| fd9b311095 | 原生 UUID / 原重试索引 / 512 批次 | 124.20 | 13.33 | 保留较低 I/O 实现，尚未通过双指标门槛 |
| e2e3873e52 | 原生 UUID / 1024 批次 | 94.93 | 不采信 | TPS 阶段结束后，末段投递遇到休眠；回退至 512 |

稳定索引实验虽然出现了 127,109 次 HOT 更新，但堆表块读取由 99,434 增至 665,265，
因此没有保留它。HOT 需满足不更改索引引用列和页内有空间等条件，不能单看 HOT 次数判断收益；
参见 [PostgreSQL 15 HOT 文档](https://www.postgresql.org/docs/15/storage-hot.html)。
原生 UUID / 512 对照的投递索引块读取由 633,593 降至 390,356，
投递表及索引总大小由 481,968,128 降至 334,503,936 字节。
这些是本轮累计数据库统计，不等同于物理磁盘 I/O，且 WAL 统计包含测试启动和恢复步骤。
报告位于对应的 `target/notification-test-*` 目录，新增 `delivery-io-after-load.json` 与
`delivery-io-final.json`，记录实际索引、表选项、HOT、块读取和 WAL 统计。

重测时发现 macOS 自动休眠影响了 `70a975287b`，该轮虽然正确性通过，
但通知墙钟延迟不可用；原始报告保留，并另存 `performance-validity.json` 与 `power-events.log`。
`e2e3873e52` 的末段投递也覆盖了短暂休眠，因此不采用该轮 P95；早先输出的比较文件不代表最终性能结论。
测试脚本现使用随进程退出的临时 `caffeinate` 防止自动休眠，不修改系统永久电源设置，
并保存 `clock-safety.json`，检查墙钟与单调时钟偏差及保护进程存活状态。
时钟异常时保留正确性证据，但标记 `performance_valid=false` 并拒绝性能比较；强制休眠仍可能中断测试。

防休眠后的有效原始客户端重测：`target/notification-test-30fec46f58`，
10 站点 / 60 秒 / 每篮 1–20 件 / 100 订阅者，10,340 笔交易，**172.16 TPS**，
通知 P50 **50.49 秒**、P95 **78.61 秒**、P99 **81.09 秒**。
4,806,403 条通知全部正确送达，操作错误、缺失、多余、错误数量、重复日志均为零，
独立计数器审计及重启/离线/重放检查通过，时钟偏差约 -0.01 秒，`performance_valid=true`。
上一轮缓存关闭的 `d9eccff4cc` 为 188.37 TPS / P95 188.92 秒 / 5,267,152 条通知；
新结果延迟较低但吞吐仍低于该参考，也没有恢复历史事件服务约 264 TPS。
两次原始客户端运行产生的通知总量不同，不能把百分比差异都归因于代码。
结论：存储/索引 I/O 降低得到固定量证据支持，**同时降低 P95 并恢复 TPS 的整体目标仍未达到**。

已停止测试的 PostgreSQL/Kafka 二进制临时数据在确认进程退出后清理；报告、SQL/服务日志保留。
部分订阅 JSONL 使用 `.jsonl.gz` 无损压缩并校验解压后的 SHA-256，详情见各运行的
`subscriber-log-archives.json`；需要再次读取原始 JSONL 的工具时，请先解压副本并预留空间。

本轮主要修改文件：
- `notification-service/database/schema.sql`：原生 UUID 与事务性迁移。
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`：发送/ACK 的 UUID 批量匹配。
- `notification-service/src/main/java/edu/cs6510/notification/SubscriberHandler.java`：拒绝格式错误的 UUID ACK。
- `scripts/notification_metrics.py`、`scripts/stage_diagnostics.py`：I/O 快照、UUID 查询和旧行宽度对照修正。
- `scripts/benchmark_clock.py`、`scripts/test-notifications.py`：临时防休眠、时钟有效性验证及证据保存。
- `scripts/compare_fixed_runs.py`：拒绝时钟无效或事后判定受污染的运行。
- 对应 Java/PostgreSQL/Python 测试与本 README；未修改老师 load-client。

### 可选的销售内容缓存（默认关闭）

`NOTIFICATION_PAYLOAD_CACHE_SIZE=16384` 可开启有界 LRU，只缓存共享销售商品内容；
`0` 使用原来的数据库 JOIN 路径，上限为 65536 条。订阅者归属、待发送资格、重试时间、
发送计数和 ACK 仍由数据库判定，不缓存投递状态。每批最多一次批量补齐未命中的内容；
不会为每条通知单独查询，缓存淘汰也不会丢弃通知。

`GET /cache-metrics` 返回当前进程的容量、大小、命中、未命中和淘汰计数，
不混入持久化正确性计数。测试将其保存为 `payload-cache-metrics.json`。
服务重启后缓存和缓存统计从零开始，未确认通知仍从数据库恢复。

启用前须停服务并运行当前 schema；新增的数据库触发器禁止修改已写入的共享商品内容。
执行数据库恢复、重建、重置 identity 或其他维护操作时必须停止并重启通知服务，
不能让旧进程缓存跨数据库生命周期使用。不要用禁用约束/触发器的方式在线更改销售内容。

固定量对照：`target/notification-test-0836709052`（缓存关闭）与
`target/notification-test-cce4d1518e`（16384 条缓存）均为 2000 笔销售、每篮 10 件、
10 个站点、100 个订阅者，精确产生 1,000,600 条通知，正确性均通过。
TPS 为 102.74 → 103.37，通知 P95 为 11.96 → 11.55 秒；收益很小，单次顺序实验不能证明稳定提升。
这里使用独立固定量驱动，**不是老师原始 load-client，也不能直接与原始客户端 TPS 比较**。
缓存暂不默认启用；它不能消除投递状态更新和索引的数据库 I/O。

原始 load-client 的 10 站点 / 60 秒 / 每篮 1–20 件 / 100 订阅者验证保存于
`target/notification-test-0d0833ea4f`：7008 笔交易，116.71 TPS，通知 P95 为 92.74 秒，
3,275,787 条通知全部正确送达，操作错误、缺失、错误数量和重复日志均为零。
相较上一轮 `d9eccff4cc` 的 188.37 TPS / P95 188.92 秒，吞吐并未恢复，且两轮通知总量不同，
不能把延迟差异全部归因于缓存。此次实验**没有达到同时恢复 TPS 和降低 P95 的目标**。
现场快照 `live-database-io.txt` 中投递状态索引有 1,029,806 次块读取、共享商品索引只有
3,985 次块读取；这只是一个时点的累计数据库统计，不是物理磁盘读取或完整耗时归因。
它说明下一步应重点验证投递状态更新、重试索引维护和清理压力，而不是继续放大内容缓存。

```text
load-client -> HTTP Gateway -> Kafka request/reply -> Checkout
                                                     |
                              COMPLETED + sale_outbox (atomic)
                                                     |
                                              Kafka sales
                                                     |
                                          Notification Service
                                         Inbox + Delivery (atomic)
                                                     |
                                    WebSocket connection per user
                                                     |
                                  Subscriber threads -> JSONL -> ACK
```

Kafka 保存事件；WebSocket 是交付通道。可靠性来自 Outbox、Inbox、持久 Delivery 和客户端 ACK，
不是来自 WebSocket 本身。交付语义是至少一次，客户端幂等记录，不宣称 exactly-once。

## 前置条件

Java 21+、Maven（或 Maven Wrapper）、Python 3、PostgreSQL 15+、Kafka。
依赖版本沿用原项目，不进行额外版本升级。

## 一键隔离验证（推荐）

在本目录执行：

```bash
python3 scripts/test-notifications.py --subscribers 100 --duration 10
```

脚本默认读取同级原项目 target/kafka-dist 中的 Kafka 可执行发行版，不读取或修改原 Kafka 数据。
若该发行版不存在，可指定 --kafka-distribution；现有 helper 也支持从本地官方 Docker 镜像提取。
PostgreSQL 默认使用 /opt/homebrew/opt/postgresql@15/bin，可通过 --pg-bin 修改。

脚本会：

1. 检查端口与剩余空间，在本目录 target 下创建唯一测试目录。
2. 默认每个服务一个专属 PostgreSQL 实例（25432–25436），每实例 shared_buffers=32MB；
   fsync 和 synchronous_commit 保持开启。--database-servers shared 可切换为单实例。
3. 运行 Maven package，包括真实 PostgreSQL 测试。
   新建的隔离测试数据库中，每 SKU 库存默认为 1,000,000（--opening-stock 可配置），
   与历史性能测试一致；不修改旧数据库、业务库存校验或 seed.sql。
4. 启动独立 Kafka（29092）、六个服务（18180–18185）和 100 个 WebSocket 订阅者。
5. 验证仅扫描不通知、重复完成、离线交付、通知服务重启及 Outbox 重放。
6. 编译并运行同级原 load-client 的源码，输出写入新测试目录，不修改原客户端。
7. 根据 Checkout 的已完成交易与商品明细独立计算预期通知，核对日志。
8. 写 verification.json；退出时关闭本次启动的进程，保留测试证据和数据。

PostgreSQL 需要创建共享内存，某些应用沙箱不允许；这种情况下需在普通终端运行或批准提权。
测试数据库使用仅监听本机的 trust 认证，仅适用于个人本地测试。

## 性能对照（顺序运行）

### 分阶段诊断

```bash
python3 scripts/test-notifications.py --subscribers 100 --stations 10 --duration 10 --min-items 1 --max-items 20 --drain-timeout 180 --diagnostics
```

运行结束生成 stage-diagnostics.json，提供下列毫秒统计：
- sale_to_publish_start：成交到成功发布尝试开始（每笔销售）。
- publish_to_broker_ack：发布到 broker ACK；这段与下项重叠，不可重复相加。
- publish_start_to_consumer_callback：发布、Kafka 等待及消费者调度（每笔销售）。
- consumer_callback_to_commit_observed：连接池等待、锁、通知生成及数据库提交（每笔销售）。
- commit_observed_to_dispatch：提交后到发送开始（每条通知）。
- dispatch_to_client_record：发送、网络与客户端处理到记录时间（每条通知）。
- client_record_to_ack_update：日志记录到 ACK 的数据库更新时间，使用固定种子的最多2048条抽样。

诊断记录消费者事务返回时间；并发发送线程可能略早于该观察时间看到已提交数据，
因此保留小负值并显示 negative_count，不暗中截断。
不同阶段采用销售/批次/通知不同权重，不能相加各阶段的P95来推算端到端P95。
本轮含恢复探测，不是纯稳态性能测试。诊断增加时间戳和少量事件日志，不改变订阅范围或交付语义。
Java相关模块测试和21项Python回归已通过；新增诊断的真实运行因自动提权审批额度失败未执行，
需要普通终端运行上面的命令。不能把先前没有分阶段计时的测试结果当成本次诊断结果。

默认商品数已改为 2000，每 SKU 库存 1,000,000；
服务 JVM 为 -Xms64m -Xmx256m -XX:TieredStopAtLevel=1，与历史报告配置对齐。
先检查磁盘空间、接通电源，关闭低电量模式。三轮不要同时运行。

```bash
python3 scripts/test-notifications.py --variant original --subscribers 0 --stations 10 --duration 60 --min-items 1 --max-items 20
python3 scripts/test-notifications.py --variant notification --subscribers 0 --stations 10 --duration 60 --min-items 1 --max-items 20 --drain-timeout 600
python3 scripts/test-notifications.py --stations 10 --duration 60 --min-items 1 --max-items 20 --subscribers 100 --drain-timeout 600
```

第一轮把同级原 event-driven-server 的当前源码复制到本次 target 的临时目录再编译运行，
不修改旧作业，不使用旧 target 的 JAR。第二轮隔离新增销售事件和通知消费的固定开销；
第三轮再加完整 100 用户扇出。无订阅者模式不执行通知恢复核验，notification_passed 为 null。
每轮完成后检查磁盘空间，再运行下一轮。启动要求至少 3 GiB，负载/排空期间低于 1 GiB 会中止并清理进程，
不自动删除任何旧报告。通知可能有较长积压，超时代表未在指定时间内排空，
不应掩盖为成功。负载结束后立即打印客户端 TPS，保存 throughput.json 和原始 load-report。
此 TPS 不包含随后等待通知和重放验证的时间。正确性核验按用户处理，避免一次构建所有用户的通知集合。
verification.json 中 notification_passed 只表示通知正确，load_passed 表示有成功成交且全部操作零错误；
顶层 passed 要求两者同时成立。负载失败仍继续核验通知，最后以非零状态退出。
即使之后积压排空超时，原始 load-report、throughput.json、workload-validation.json 已保留。
库存设置记录在 inventory-setup.json；该值是验证交易开始之前的初始库存。
environment.json 记录源文件哈希、客户端哈希、商品数量、数据库及 JVM 配置。
原版有四个数据库实例，新版增加第五个通知实例，这是新增功能的真实资源成本。
不要把当前原版源码的测量当作历史 commit 的精确复现：系统负载、供电状态、JDK 等仍影响结果。
建议每轮重复三次取中位数；100 用户模式还有负载前恢复检查，短时测试不适合据此推导稳定容量。
100 个通知用户与 100 个结账站点是两个独立参数。历史报告没有通知扇出负载。
只有这三组正确性及延迟可接受后，再将站点/时长改为 100/120 做 Stress。

### 保留旧条件比较优化效果

之前 65.55 TPS 的运行使用 20 SKU、共享数据库和 192MB JVM 堆。
需要只观察代码优化、而不混入部署调整时，可显式指定：

```bash
python3 scripts/test-notifications.py --stations 10 --duration 60 --min-items 1 --max-items 20 --subscribers 100 --drain-timeout 600 --catalog-size 20 --database-servers shared --java-opts '-Xms32m -Xmx192m -XX:TieredStopAtLevel=1'
```

## 手动启动

1. 创建 pattern_catalog_db、pattern_checkout_db、pattern_inventory_db、
   pattern_analytics_db、pattern_notification_db，以及对应的私有数据库账号。
2. 复制 .env.example 为 .env，填写账号密码；不要提交 .env。
3. 执行 docker compose up -d kafka，或运行原生 Kafka helper。
4. 执行 ./run.sh。脚本初始化表、应用成交 Outbox migration、构建并启动服务。

默认 Gateway：HTTP 8180；Notification：WebSocket 8185。
Compose Kafka 使用 29092，topic prefix 为 systemdesign，与原作业默认配置隔离。
不要把新目录的数据库配置指向旧作业数据库。

另开终端运行：

```bash
java -jar subscriber-client/target/subscriber-client-0.0.1-SNAPSHOT.jar \
  --subscribers 100 --seconds 180 \
  --skus SKU-000001,SKU-000002,SKU-000003 \
  --prefix demo --output target/subscribers
```

偶数编号订阅给出的全部 SKU，奇数编号订阅其中一个，便于演示不同订阅关系。
看到 READY 后，再运行原 load-client，将 baseUrl 设置为 http://127.0.0.1:8180。
在客户端停止前等待 http://127.0.0.1:8185/metrics 的 pending 降至 0。

```bash
python3 scripts/verify_notifications.py --env-file .env --logs target/subscribers
```

核验假定日志覆盖该数据库中的全部订阅者及其订阅以来的销售；重复演示建议使用独立测试实例。
重新连接同一个用户必须保留原输出目录中的 token 文件和原 SKU 列表。

## WebSocket 协议

连接 /notifications，发送：

```json
{"type":"SUBSCRIBE","subscriberId":"producer-1","token":"至少32字符的随机凭据","skus":["SKU-000001","SKU-000002"]}
```

服务端确认 SUBSCRIBED；之后发送 SALE，字段包括 notificationId、eventId、subscriberId、
transactionId、sku、quantity、completedAt。客户端记录后发送：

```json
{"type":"ACK","notificationId":"收到的通知ID"}
```

错误返回 ERROR 并关闭连接。每个身份同时只允许一个活动连接，订阅创建后保持不变。
只交付 completedAt 不早于 subscribed_at 的销售，避免首次订阅时推送历史成交。
本地部署假定数据库与服务时钟一致；跨机器部署需统一时钟和明确订阅时间边界。

## 指标与限制

/metrics：注册用户、活动连接、已接收成交数、通知数、待确认数、发送尝试数。
verification.json：漏发、错发、数量错误、重复日志，以及端到端延迟 P50/P95/P99。
故障恢复测试的延迟包含故意离线时间，不应解释为纯稳态性能。

目前是单实例通知服务，适合本作业规模，不宣称多实例连接路由已实现。
发送批次有上限、连接缓冲有上限，慢客户端不会在 Kafka 回调中直接发送。
优化后每连接最多取 128 条，有积压则继续发送，仅空闲时等待 200ms。
每批发送状态一次 UPDATE；ACK 保存在每连接最多 1024 项的有界队列，发送线程批量提交。
ACK 到达内存不视为持久确认：崩溃或断线丢失该队列时，数据库中未确认消息仍会重投。
每笔销售的 SKU 匹配及通知生成改为一条 INSERT SELECT；订阅写入使用 JDBC batch。
这些优化不关闭事务、fsync、重试或客户端去重，也不减少订阅者的订阅范围。
持久化通知和客户端去重日志没有自动清理策略；长时间生产运行需增加保留期限。
毒消息会保留并重试，生产环境应增加隔离队列和告警。
token 用于恢复用户身份，客户端仅本地保存；不是完整注册/授权系统。
服务默认绑定本机，面向公网还需要 TLS、账户认证及权限控制。

## 当前验证状态

### 接收流控与重连修复（2026-11-17）

60秒运行 e31b30665d 的结账负载为130.89 TPS、零错误，但通知排空超时。
订阅者日志确认部分线程遇到旧连接占用后永久退出，因此该轮不算通知验证通过。
修复新建 subscriber-client（不是老师的 load-client）：
完整消息处理、日志记录及 ACK 后才请求下一条消息；分片可继续请求后续帧。
服务端返回明确的 SUBSCRIBER_BUSY/CHANNEL_BACKPRESSURE/TEMPORARY_FAILURE 错误码，
客户端对此重连；凭据错误等不可恢复错误则让整个客户端及时失败。
测试脚本保存 drain-status.json，失败时保存 failure.json，并检查客户端进程是否仍然存活。

修复后真实集成运行 target/notification-test-59aee12159：
10个结账站点、10秒负载、100订阅者、2000 SKU，634,668条通知全部正确交付，
pending=0，100连接保持在线；139项Java测试无跳过，18项Python回归通过。
但通知P95仍约69.4秒，负载到核验总耗时约91.7秒，说明服务消费/投递容量仍落后于产生速度。
短时通过不代表60秒运行已验证通过，也不代表性能问题已经解决。

- Maven 编译、打包成功。
- 优化后真实 PostgreSQL 环境 Maven package：134 项测试全部通过，无跳过。
- Python 脚本回归：18 项通过。
- 用户在普通终端完成首次 100 订阅者验证：539 笔成交、52,475 条通知，漏发、错发、重复日志为零。
- 后续 60 秒测试暴露测试库存仅为 1000 的问题：通知核验通过，但结账出现库存拒绝，不可作无缺货吞吐量基准。
- 修正库存后的旧实现：3944 笔负载成交、65.55 TPS、零请求错误，但通知 P95 约 376 秒，存在明显积压。
- 批量优化后已完成 10 用户、2000 SKU、2 站点/3秒短时验证：4785 条通知准确交付，P95 约 280ms。
  这不是与上条相同负载，不据此声称吞吐量提升比例。完整 60 秒对照仍需重跑。
- 原版、新版零订阅者、新版100订阅者三种短时模式均已验证；100用户准确交付49,512条通知。
  优化验证摘要见 docs/optimization-validation.md，正式60秒对照尚未运行。

## Load client 来源

本次作业没有修改同级 load-client 的 Java 源码。与 gortonator/CS6510-2026 main 的
Git tree 0e05e5a8d9fa9a7ffb65c92d442c16bd22448e06 逐文件比对：
9 个 Java 源文件及 build.sh、run.sh 的 Git blob 哈希全部一致。
本地两个 shell 脚本相对本地 Git HEAD 仅有可执行位变化（100644 -> 100755），没有内容改动。
本次 runner 将原客户端编译输出和报告写入自己的 target 测试目录。

完整新增/修改文件清单见 CHANGES.md。

## 通知消费小事务

通知服务将每次 Kafka 回调拆成最多 16 笔销售的事务，通过
`NOTIFICATION_INGEST_BATCH_SIZE` 可配置为 1–256。每个小批次单独提交，
使发送线程不必等待整个 Kafka 批次完成才看到已提交通知。
Kafka offset 仍在整个回调成功后确认；失败重放时已提交销售由 inbox 去重。
这不改变 SKU 订阅、持久化、ACK 或原始 load-client，也不保证总吞吐量一定提升。

`--diagnostics` 将 Kafka 回调等待、回调内的小批次等待、事务耗时分开报告，
并增加 advisory-lock 查询、inbox SQL、通知 fanout SQL 耗时。
SQL 计时不含事务连接获取和提交，不应视为完整事务或独立连接池等待时间；
新销售与纯去重批次分开统计，避免重放的小事务掩盖真实写入开销。

本次验证 `target/notification-test-7f4c7893b1`：147 项 Java 测试通过（无跳过），
22 项 Python 测试通过；10 站点/10 秒/100 订阅者，879 笔负载成交、87.02 TPS，
请求错误为零；含恢复探针共 881 笔销售、432,225 条通知准确交付，无漏发、错发、
数量错误或重复日志，pending=0。端到端 P50=37.54 秒、P95=56.56 秒。
相比上一轮 152.61 TPS，本轮吞吐量更低；通知数量也不同，不能单凭延迟下降宣称优化成功。
小事务可靠性已验证，但连接池/数据库并发竞争仍需进一步定位；此结果不是完整 60 秒性能验收。

启用 `--diagnostics` 时，`PoolDiagnostics` 每秒读取 Hikari 的 active/idle/total/waiting
计数，不额外借用数据库连接；报告的 `connection_pool` 汇总等待样本和最大等待线程数。
这些是连接/线程数量，不是毫秒；采样覆盖启动、压测及排空，可能漏掉短暂争抢，
无等待也不能排除 SQL、CPU 或磁盘竞争。未开启诊断时不创建采样组件。
原版普通 Kafka 消费最多 256 条记录/poll，RPC 消费最多 64 条/poll；
原版 analytics 将扫描事件重新组合为每事务最多 1,000 个扫描事件。
通知的 16 笔销售/事务是试验配置，不是原版参数，也不是已验证的最优值。

连接池诊断运行 `target/notification-test-b648068ce9`：149 项 Java 测试无失败/跳过，
23 项 Python 测试通过。100 订阅者、10 站点、10 秒负载为 90.42 TPS，操作错误为零；
含探针 915 笔销售、432,119 条通知核验通过，P95=54.36 秒。
80 个连接池样本中 58 个有等待，最大 98 个等待线程；等待线程 P50=82。
样本覆盖启动和恢复，不代表纯负载期百分比；排空阶段也观察到 8 个连接全占用及
84–94 个等待线程。连接池竞争已被观察到，但扩大池容量的效果尚未验证，不能据此排除磁盘或 CPU 瓶颈。

### 发送侧数据库并发限制

`DispatchDatabase` 使用公平、可中断的 semaphore，在调用 `NotificationStore` 事务代理之前
获取名额，代理返回（包括 ACK 提交）后 finally 释放。pending 查询、sent 更新和 ACK 写入
共享默认 4 个名额，通过 `NOTIFICATION_DISPATCH_DB_CONCURRENCY` 调整；配置必须至少为
连接池留下 2 个名额。池大小仍为 8，销售小事务仍为 16，不改变可靠性或订阅规则。
限流不覆盖 WebSocket 发送、等待客户端 ACK 或空闲休眠；消费写入不经过此 semaphore。
这是单服务进程内限制，并非专用连接预留：订阅注册、指标查询仍可使用共享池。
诊断的 `dispatch_gate_waiting_threads` 统计应用层排队，与 Hikari 等待分开，
避免把等待从数据库连接池移到 semaphore 误认为所有拥堵已消失。

验证运行 `target/notification-test-bb4b549d9d`：153 项 Java 测试无失败/跳过，23 项
Python 测试通过。与上一轮相同配置（100 订阅者、10 站点、10 秒、每篮 1–20 件）：
1237 笔负载成交，123.05 TPS，零操作错误；含探针共 1239 笔销售、594,074 条通知，
漏发/错发/数量错误/重复日志均为零，pending=0，恢复及重放检查通过。

| 指标 | 无发送侧限流 b648068ce9 | 限制并发为 4 bb4b549d9d |
|---|---:|---:|
| 结账 TPS | 90.42 | 123.05 |
| 通知数量（含探针） | 432,119 | 594,074 |
| 通知 P50 秒 | 26.91 | 55.97 |
| 通知 P95 秒 | 54.36 | 60.70 |
| Hikari 等待线程采样 P50 | 82 | 0 |
| 应用层限流等待线程采样 P50 | 未采集 | 95 |
| 销售小事务耗时 P50 毫秒（按销售加权） | 983.51 | 556.46 |

该单次对照显示连接池竞争降低、结账吞吐量增加，但发送等待和端到端延迟变差。
负载发生器是闭环、两轮销售/通知数量不同，不能声称严格受控的提升比例或最佳并发值。
这不是整体性能验收通过；保留并发可配置，后续需在足够磁盘空间下做重复或固定事件集对照。

### 发送循环合并与批次

发送循环将上一批发送状态、已收到 ACK 和下一批查询合并为一个 `dispatchBatch`
事务，统一经过发送侧 semaphore。网络发送始终在提交并释放名额后执行。
若发送或事务失败，未确认的持久化通知保留并可重发；关闭连接时尽力补记已发送状态，
客户端继续按 notificationId 去重。没有把网络发送纳入数据库事务。

发送查询默认上限从 128 改为 512，可用 `NOTIFICATION_DISPATCH_BATCH_SIZE` 配置 1–512；
ACK 队列仍为 1024，并发上限仍为 4，消费每事务仍为 16 笔销售。
新增真实 PostgreSQL 测试覆盖发送/ACK 同事务回滚、512 条查询上限及剩余通知不丢失。
仅合并事务的中间运行 `7cd0431bbb`：433,024 条通知核验通过，89.96 TPS，P95=53.50 秒；
由于销售/通知数量不同，不能直接把该延迟差视为因果改善。

本次已确认旧测试停止后清理其 PostgreSQL/Kafka 数据目录，释放约 9.94 GiB；
其余 4,457 个文件保留并核对存在及大小，包括报告、服务日志、订阅日志和配置。
Kafka 数据目录内的 `.log` 是消息段，随临时数据删除；外部 Kafka 运行日志保留。
系统已有的 Homebrew PostgreSQL 服务未停止、未清理。

512 条发送批次运行 `target/notification-test-1ba5bc49ec`：157 项 Java 测试全部通过
（包括真实 PostgreSQL，无跳过），23 项 Python 测试通过。100 订阅者、10 站点、
10 秒相同参数下，81.75 TPS、零操作错误，377,119 条通知完全匹配，pending=0，
恢复/重放验证通过；通知 P50=50.01 秒、P95=50.91 秒。
与修改前 `bb4b549d9d` 的 123.05 TPS、594,074 条通知、P95=60.70 秒相比，
本轮延迟较低但成交和通知量也更低，不能宣称整体优化成功或512优于128。
连接池等待 P50 仍为0，应用层等待 P50=95，表明发送队列仍然拥堵。
512 是待进一步受控验证的配置，可通过环境变量恢复128；没有修改原 load-client 或原版服务。

## 固定通知量定位与当前配置

当前默认：发送数据库并发 **1**，发送批次 **512**，销售消费事务 **16**；
`NOTIFICATION_DISPATCH_CUSTOM_PLAN=true`。历史试验参数与结果保留在前面各节。

定位证据：`notification-test-d7bb7fb6b6/services/notification.log` 中
`NOTIFICATION_SLOW_ACK_PLAN` 记录了慢 ACK 所在 JDBC 连接的实际缓存计划，
`genericPlans=68, customPlans=5`，包含 `Seq Scan` 和 `Nested Loop Semi`。
该通用计划仍按很小的行数估算执行，而通知表已快速增长。固定基线 ACK P95 达到
194–368ms，取下一批查询 P95 仅 5.54–9.90ms；因此先修复 ACK 慢计划，而不是继续扩大池。

修复在写入型发送事务开始时执行 `SET LOCAL plan_cache_mode=force_custom_plan`，
让当前参数重新规划，事务结束后恢复原设置。只读空轮询不额外执行设置，
其他服务和数据库全局配置不变，仍保留落盘、提交后确认、失败重放和去重。
`NOTIFICATION_DISPATCH_CUSTOM_PLAN=false` 可用于回归对照。
自建 subscriber-client 的 SKU 校验改为等价集合查找，订阅内容、日志及 ACK 协议不变。

固定量测试（**独立诊断驱动，不是老师的原始 load-client**）：

```bash
python3 scripts/test-notifications.py --subscribers 100 --stations 10 \
  --fixed-sales 600 --fixed-items 10 --drain-timeout 600 --diagnostics
```

固定 600 笔销售，每笔 10 个确定 SKU，10 个站点确定分工，2000 SKU 和相同订阅规则。
每轮恰好 300,250 条通知（含两个恢复探针），保存完整商品序列及 SHA-256，
并与数据库独立核验得到的销售/通知数量匹配。任一 HTTP 失败会使运行失败，不重试凑数。
不传 `--fixed-sales` 时仍使用未经修改的原始 load-client。

新增 `scripts/compare_fixed_runs.py` 检查固定序列、通知数量、关键环境及原客户端源码一致；
只有正确性通过、候选 TPS 中位数不低于基线、P95 中位数更低时才报告 `passed=true`。
这是短时本机闭环测试，不等于严格固定到达率、长期稳定性或统计显著性证明。
执行计划诊断只执行 EXPLAIN，不执行诊断用 UPDATE；慢计划采样失败在 savepoint 内回滚。

最终两轮与两轮基线相比，每轮均为 300,250 条通知且正确性全部通过：
通知 P95 中位数从 **52.01 秒降至 16.15 秒（降低 68.95%）**；
结账 TPS 中位数从 **97.45 降至 93.59（降低 3.96%）**。
严格的“降延迟且吞吐不降”双指标验收仍未通过，不能宣称完整性能问题已解决。
160 项 Java 测试（含真实数据库、无跳过）及 28 项 Python 测试通过。
完整实验、所有中间配置与复现命令见 `docs/fixed-notification-comparison.md`，
机器可核验结果为 `docs/fixed-notification-comparison.json`。

### 后续：在低延迟下恢复吞吐量

当前版本进一步将发送状态与 ACK 合并为单次 UPDATE，避免重叠通知被更新两次；
空轮询使用 200–1000ms 有界退避，有积压时立即继续。空闲用户可能额外等待最多1秒才轮询，
可设置 `NOTIFICATION_MAX_IDLE_POLL_MS=200` 恢复原轮询频率。
`NOTIFICATION_COALESCE_DISPATCH_WRITES=false` 可恢复旧的两次写入路径做对照。

三轮固定量测试均准确交付 300,250 条通知；中位数 **97.55 TPS / P95 15.74秒**。
原固定量基线为 97.45 TPS，上一低延迟版本为 93.59 TPS / P95 16.15秒。
三轮 TPS 范围为94.48–101.86：中位数验收通过，但不能保证每轮都高于基线或外推到60秒压测。
165项 Java 测试（含真实数据库、无跳过）和29项 Python 测试通过。
完整数据、取舍与复现见 `docs/notification-throughput-restoration.md`。

### 原始 load-client 的 60 秒验证：通知排空超时

运行 `notification-test-198dcda69a` 使用未修改的老师客户端：10站点、60秒、每篮1–20件、
100订阅者，不使用固定量驱动或额外阶段诊断。完成6,956笔交易，**115.81 TPS**，
三类请求错误均为0；但通知等待600秒仍未排空，**整体验收失败**。
最后快照还有312笔销售未进入通知服务，307,708条已生成通知未确认。
仅已收到日志的P95为501.66秒，不能视作全量通知延迟，完整正确性核验未执行。
这表明固定量短测结果不能直接外推到持续60秒的负载。
清理已停止旧测试释放约6.22GiB，报告与日志保留；本次失败现场也已保留。
详见 `docs/original-load-client-60s-notifications.md`。

### 后续：移除监控全表扫描，原始客户端重测

已定位 `/metrics` 持续扫描数百万条通知的问题，改为事务内精确计数器；
实测查询执行时间从旧现场的425–4287ms降至新现场的0.103ms。
新增独立全表审计和回滚/重放/重复ACK回归测试，不更改600秒超时或订阅范围。
同参数重测完成11,562笔交易，**192.54 TPS**，三类请求均零错误；
但排空期间触发低于1GiB的磁盘安全保护，**整体验收未通过**。
消费/发送仍有数据库I/O积压，不能宣称低通知延迟或恢复历史吞吐基线。
169项Java测试（无跳过）及31项Python测试通过；所有报告与日志保留。
完整证据、修改文件和剩余瓶颈见 `docs/notification-metrics-bottleneck.md`。

### 后续：订阅覆盖索引与投递索引减负

订阅匹配改用 `(sku,subscriber_id)` 覆盖索引；去掉投递表冗余唯一索引，
仍通过同一事务内的销售 inbox 去重门禁保证重放不重复生成通知，并新增并发重放测试。
清理已停止的数据库/Kafka临时数据，保留旧报告和日志后，用老师原始客户端同参数重测：
**7,321笔交易 / 121.88 TPS / 请求零错误；3,434,049条通知全部正确收到，pending=0。**
独立核验、重启恢复、outbox重放及精确计数审计全部通过，600秒上限未改动。
但通知P95仍为**308.62秒**，TPS比上一轮192.54下降36.70%，不能宣称低延迟和吞吐目标已达成。
171项Java测试（无跳过）及31项Python测试通过。部署迁移注意事项、数据和剩余瓶颈见
`docs/notification-io-optimization.md`。

### 后续：批量 SQL、UUIDv7、批量通知与 ACK

同参数老师原始客户端重测 `a64b2150b2`：**11,971笔 / 199.30 TPS / 请求零错误**；
**5,656,556条通知全部正确交付**，重放、恢复及计数审计通过，pending=0。
相对上一完整运行121.88 TPS / P95 308.62秒，TPS提高63.52%，P95降到253.13秒（降低17.98%）；
但P50有所上升，历史264.24 TPS仍未恢复，不能称为低延迟问题已完全解决。
保留事务语义，新增真正批量写入、有序通知主键、日志先刷新再批量ACK，以及可协商的32条通知批次。
隔离测试的通知数据库默认使用LZ4 WAL压缩，仍启用fsync、同步提交和整页写保护；
32MB缓存、原JVM参数、100订阅者及600秒排空上限不变。原始load-client未修改。
186项Java测试（无跳过）和33项Python测试通过。所有探索结果、限制、变更文件和日志归档说明见
`docs/notification-throughput-latency.md`。

### 后续：共享销售内容与紧凑投递状态

新增 `notification_sale_items`，每个成交SKU保存一份共享内容；投递表仅保留引用和发送/ACK状态。
发送先限量取512条状态，再关联内容；新增事务化旧表迁移，保留ID、ACK和重试时间，不一致时完整回滚。
**现有数据库须先停止通知服务并备份，再执行新schema；不要混跑旧服务与新表结构。**
同参数原始客户端测试 `d9eccff4cc`：526.7万条通知全部正确收到，190项Java测试（无跳过）及34项Python测试通过。
采样投递行约100字节，对比旧结构等价行188字节；P95从253.13降到188.92秒。
但TPS从199.30降到188.37，P50也上升，**尚未满足降延迟且吞吐不下降的双指标**。
迁移限制、完整证据和变更文件见 `docs/compact-notification-state.md`。
