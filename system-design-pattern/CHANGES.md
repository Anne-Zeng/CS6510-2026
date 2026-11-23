# 修改和新增文件

以下路径相对于本目录；对比来源为同级 event-driven-server 的本地工作副本，而非纯净 Git HEAD。
原目录和原 load-client 未修改。

共享销售内容/紧凑状态改造新增数据库表 `notification_sale_items`，修改批量入库和限量关联读取，
增加事务化旧结构迁移及回滚测试。精确变更和本轮性能限制见 `docs/compact-notification-state.md`。

最新吞吐/延迟优化新增 `subscriber-client/src/main/java/edu/cs6510/subscriber/ReceiptBatch.java`、批量ACK/通知及UUID迁移回归测试，
并修改通知批量入库、ID默认值、可协商WebSocket批次和隔离测试WAL压缩配置。
精确文件清单、完整原始客户端结果及未达到的性能目标见 `docs/notification-throughput-latency.md`。

## 新增

- `checkout-service/database/006-sale-outbox.sql`
- `checkout-service/src/main/java/edu/cs6510/servicebased/checkout/SaleOutboxPublisher.java`
- `checkout-service/src/test/java/edu/cs6510/servicebased/checkout/persistence/SaleCompletionPostgresTest.java`
- `event-messaging/src/main/java/edu/cs6510/events/SaleCompleted.java`
- `event-messaging/src/test/java/edu/cs6510/events/SaleCompletedTest.java`
- `notification-service/database/schema.sql`
- `notification-service/pom.xml`
- `notification-service/src/main/java/edu/cs6510/notification/NotificationApplication.java`
- `notification-service/src/main/java/edu/cs6510/notification/NotificationStore.java`
- `notification-service/src/main/java/edu/cs6510/notification/Protocol.java`
- `notification-service/src/main/java/edu/cs6510/notification/SalesEvents.java`
- `notification-service/src/main/java/edu/cs6510/notification/StatusController.java`
- `notification-service/src/main/java/edu/cs6510/notification/SubscriberHandler.java`
- `notification-service/src/main/java/edu/cs6510/notification/WebSocketConfig.java`
- `notification-service/src/main/resources/application.properties`
- `notification-service/src/test/java/edu/cs6510/notification/NotificationStorePostgresTest.java`
- `scripts/test-notifications.py`
- `scripts/verify_notifications.py`
- `subscriber-client/pom.xml`
- `subscriber-client/src/main/java/edu/cs6510/subscriber/SubscriberMain.java`
- `subscriber-client/src/test/java/edu/cs6510/subscriber/SubscriberMainTest.java`
- `CHANGES.md`（本文件）

## 基于副本修改

- `.env.example`
- `README.md`
- `analytics-service/src/main/resources/application.properties`
- `catalog-service/src/main/resources/application.properties`
- `checkout-service/src/main/java/edu/cs6510/servicebased/checkout/SchedulingConfig.java`
- `checkout-service/src/main/java/edu/cs6510/servicebased/checkout/persistence/SagaStore.java`
- `checkout-service/src/main/resources/application.properties`
- `compose.yaml`
- `event-messaging/src/main/java/edu/cs6510/events/KafkaBus.java`
- `inventory-service/src/main/resources/application.properties`
- `pom.xml`
- `scripts/system.py`

## 未修改的复制文件

126 个源码、构建与脚本文件沿用原实现。未复制原 target、运行数据、历史报告和 .env 凭据。

## 关键修改说明

- SagaStore：成交状态与销售 Outbox 原子写入；取消不生成销售。
- SaleOutboxPublisher：Kafka broker 确认后标记完成，失败保留重试。
- KafkaBus：新增 notification 服务标识、sales topic，隔离默认前缀。
- NotificationStore：SKU 订阅、100 用户上限、Inbox 去重、持久通知与 ACK。
- SubscriberHandler：独立连接、异步发送、缓冲限制与断线恢复。
- SubscriberMain：可配置线程、独立身份、JSONL 日志及重连去重。
- scripts/system.py：五数据库、六进程、成交 Outbox migration。
- 集成验证：使用独立测试数据库，依据实际成交明细核对日志。

## 验证状态

分阶段诊断：
- checkout-service/database/006-sale-outbox.sql：新增 publish_started_at 和 broker_acked_at。
- SaleOutboxPublisher.java：在原有确认更新中保存成功发布尝试的时间，不额外逐事件查询。
- SalesEvents.java：可选消费回调开始/事务返回日志；application.properties 增加诊断开关。
- Protocol.java、NotificationStore.java、SubscriberHandler.java：通知增加 sentAt，客户端原样记录。
- scripts/stage_diagnostics.py：按阶段生成百分位、覆盖率和ACK抽样统计。
- scripts/test-notifications.py：--diagnostics 和 stage-diagnostics.json，记录开关到环境证据。
- 新增 SaleOutboxPublisherTest.java、SalesEventsTest.java、test_stage_diagnostics.py。
- 未修改老师 load-client；真实诊断待终端运行，审批额度失败不代表测试通过。

接收流控/重连修复：
- subscriber-client/SubscriberMain.java：完整消息处理后才增加接收需求；可恢复错误重连；线程失败及时上报。
- notification-service/SubscriberHandler.java：显式可恢复错误码，区分忙连接、背压和非法请求。
- subscriber-client/SubscriberFlowControlTest.java：新增5项流控、分片、突发消息和错误分类检查。
- scripts/test-notifications.py：客户端存活检查、drain-status.json 和 failure.json 诊断快照。
- README.md、docs/optimization-validation.md：更新实测结果及尚未解决的延迟限制。

新增 docs/optimization-validation.md：134项Java、18项Python测试及三模式真实短时运行记录。

性能优化及可比基准：
- NotificationStore.java：每笔销售一次 INSERT SELECT，订阅 JDBC batch，发送状态/ACK 批量 UPDATE。
- SubscriberHandler.java：128 条发送批次，有积压时不休眠，有界 ACK 队列与批量持久化。
- notification-service/database/schema.sql：增加匹配发送排序的待处理索引。
- subscriber-client/SubscriberMain.java：允许覆盖 2000 SKU 的订阅规模；原 load-client 不变。
- scripts/benchmark_runtime.py：独立 PostgreSQL 实例、统一商品种子、原版临时源码副本及低磁盘中止。
- scripts/system.py：支持四/五服务和不同源码根目录，原版基准不修改旧目录。
- scripts/test-notifications.py：原版/新版及零订阅者模式，2000 SKU、256MB JVM、独立数据库默认值。
- 新增 NotificationBatchTest.java、test_benchmark_runtime.py；扩展 NotificationStorePostgresTest.java。

库存/验收修正：
- 新增 scripts/notification_benchmark.py、scripts/tests/test_notification_benchmark.py。
- scripts/test-notifications.py：--opening-stock 默认 1,000,000，只调整本次自建测试实例；
  保存库存配置，分开 notification_passed 和 load_passed，顶层 passed 要求二者通过。
- README.md：记录上游客户端一致性检查、用户已完成的验证及本次待重跑状态。
- load-client 原目录未修改。

后续新增 scripts/tests/test_notification_verifier.py；测试脚本支持 --stations、--min-items、
--max-items、--drain-timeout，并保存独立 throughput.json；核验器改为按用户核对，降低大负载内存占用。

编译和普通测试通过；真实 PostgreSQL/Kafka/WebSocket 集成验证因执行权限审批未完成，详见 README。

通知消费小事务与诊断：
- SalesEvents.java / application.properties：默认每 16 笔销售一个事务，可配置 1–256；回调整体成功才确认 Kafka offset。
- NotificationStore.java：诊断模式记录 advisory-lock、inbox SQL、fanout SQL 分段耗时。
- scripts/stage_diagnostics.py：区分回调内等待与小事务，分别统计新销售和去重 SQL。
- SalesEventsTest.java / scripts/tests/test_stage_diagnostics.py：覆盖分批、失败传播、重放、配置边界和诊断兼容性。
- README.md：说明配置、可靠性和计时边界；未修改原 event-driven-server 或 load-client。
- PoolDiagnostics.java / PoolDiagnosticsTest.java：仅诊断模式每秒采样连接池等待，不借用连接。
- scripts/stage_diagnostics.py / scripts/tests/test_stage_diagnostics.py：连接池采样汇总与回归测试。
- DispatchDatabase.java / DispatchDatabaseTest.java：发送侧数据库操作公平限流，异常/中断释放与并发上限测试。
- SubscriberHandler.java：pending、sent、ACK 经过限流；WebSocket 发送及消费写入保持独立。
- application.properties：发送侧数据库并发默认 4；PoolDiagnostics 和报告增加应用层排队计数。
- NotificationStore.java / DispatchDatabase.java / SubscriberHandler.java：发送循环合并事务，网络发送保持事务外。
- application.properties：发送批次默认 512，配置范围 1–512；消费批次与发送并发不变。
- NotificationBatchTest.java / NotificationStorePostgresTest.java / DispatchDatabaseTest.java：批次边界、剩余交付、事务回滚与委派测试。
- NotificationStore.java：发送 SQL 分段计时、慢 ACK 缓存计划诊断、写入事务局部自定义计划。
- DispatchDatabase.java / application.properties：当前默认发送并发1；写入自定义计划默认开启。
- subscriber-client/SubscriberMain.java：SKU 校验使用等价集合查找，原 load-client 不变。
- scripts/fixed_workload.py / scripts/compare_fixed_runs.py：固定销售商品序列与严格同通知量比较。
- scripts/test-notifications.py / scripts/stage_diagnostics.py：固定量模式、独立计划数量验证、SQL计划与分段耗时报告。
- scripts/tests/test_fixed_workload.py / scripts/tests/test_compare_fixed_runs.py：确定性、失败传播、比较门槛回归测试。
- NotificationStore.java：发送状态/ACK 单次合并更新，严格 ACK 所有权校验和回滚，保留对照开关。
- SubscriberHandler.java / DispatchPollingTest.java：有界空闲退避，工作恢复时重置，有积压不等待。
- NotificationBatchTest.java / NotificationStorePostgresTest.java：重复ID去重、真实单次行更新审计、重复ACK时间戳与回滚验证。
- scripts/stage_diagnostics.py / scripts/tests/test_stage_diagnostics.py：合并写入单独计时，避免误标ACK耗时。
- application.properties / scripts/test-notifications.py：新增合并写入与空闲轮询配置并记录测试环境。
- docs/notification-throughput-restoration.md / notification-restoration-vs-*.json：三轮同通知量验收，保留所有结果与波动说明。

