# Checkout：持久化结账协调

Checkout 只访问 checkout_db，通过 HTTP 调用 Catalog 和 Inventory。当前阶段实现扫描和结账；scan_outbox 的 Analytics HTTP 投递已接入，详见 [Analytics 说明](../analytics-service/README.md)。

完成请求：`POST /transactions/{transactionId}/complete`。同一交易成功后的重复请求返回同一份收据。

| 持久化 Saga 状态 | 下一步 | 成功后 |
| --- | --- | --- |
| RESERVING | 使用 saga_id 原子扣减并确认整篮库存，再完成本地交易 | DONE |
| CANCELLING | 使用 saga_id 记录取消或释放旧版预留 | ABORTED，购物篮恢复 OPEN |
| COMMITTING（旧版恢复） | 使用 saga_id 确认旧版预留 | DONE，购物篮变为 COMPLETED |

开始协调时，在同一 Checkout 本地事务里插入 Saga 并将购物篮设为 CHECKOUT_PENDING。这个状态禁止扫描。商品明细在协调期间不可变，恢复任务可以安全地重新读取它们。每次结账尝试有独立 saga_id；取消后重新结账创建新 ID，避免复用 Inventory 的取消记录。

Inventory 操作幂等。新请求一次扣减并保存 CONFIRMED，Checkout 随后本地完成。确认不会再次扣库存；已确认库存禁止释放。明确拒绝才进入 CANCELLING；超时、连接错误或 5xx 保留 RESERVING，以相同 ID 重试，避免结果未知时错误补偿。旧版 COMMITTING 只重试确认。两个数据库之间有暂时不一致的窗口，由持久化恢复任务最终完成。

后台恢复任务默认每秒查询到期且没有有效租约的活跃 Saga。前台创建 Saga 时原子取得 30 秒数据库租约；恢复时原子接管并读取冻结购物篮。状态推进、完成和失败写入均校验租约持有者，阻止过期工作者覆盖新持有者的结果；完成或失败时清除自己的租约，进程崩溃后可在租约过期后重新接管。数据库租约不跨 HTTP 持有数据库连接。失败按 1、2、4、8、16、30 秒指数退避并加入小于一秒的抖动。

Checkout 只在实际 Inventory HTTP 调用期间限制并行数，默认 8（CHECKOUT_INVENTORY_MAX_IN_FLIGHT），本轮压测设置为 64。Inventory 新 ID 交给批量写入器，等待结果时不持有普通操作许可；现有 ID 通过事务检查幂等性。普通库存操作默认许可为 8。整篮使用有序锁与原子 SQL；多篮批次失败会退回逐篮执行。写入队列目前无界，不能将普通许可设置解释为全局队列上限。恢复任务保留退避、抖动与租约校验。

创建交易和扫描各只执行一条 SQL，直接使用 PostgreSQL 隐式事务，不额外发送 BEGIN/COMMIT。独立扫描通过原有连接池并行执行，不引入应用级扫描队列。扫描中的行锁、明细、汇总和 outbox 插入仍在同一原子语句内；数据库约束失败时全部回滚。多语句 Saga 步骤继续显式事务。

商品读取沿用上一版的不可变目录快照：Checkout 首次扫描时通过 Catalog API 加载一次目录，后续使用 Map 按 SKU 查询；并发首次扫描只加载一次。失败或空目录不永久缓存。目录初始化后固定，商品变更需要重启 Catalog 和 Checkout 刷新；已有交易保存的名称/价格快照不变。

HTTP 200 表示库存确认和 Checkout 完成均已持久化；409 表示拒绝并已完成取消、恢复购物篮；503 表示当前请求未能确认完成，客户端应查询交易或重试同一交易。即使客户端收到 503，后台仍可能稍后完成，不能创建新交易来代替重试。

## 升级已有阶段一数据库

先停止旧 Checkout，再从 microservices-server 目录执行：

```bash
psql -h localhost -U checkout_user -d checkout_db -v ON_ERROR_STOP=1 \
  -f checkout-service/database/schema.sql
export INVENTORY_BASE_URL=http://localhost:8084
java -jar checkout-service/target/checkout-service-0.0.1-SNAPSHOT.jar
```

保留之前配置的 CHECKOUT_DB_URL、CHECKOUT_DB_USER、CHECKOUT_DB_PASSWORD 和 CATALOG_BASE_URL。新数据库使用 database/schema.sql 即包含 Saga 表，无需另行升级。

## 验证

```bash
./mvnw -pl checkout-service -am package
python3 checkout-service/scripts/smoke-test.py
python3 checkout-service/scripts/saga-smoke-test.py
```

Saga 冒烟测试需要三个服务运行，且无其他库存客户端；会真实完成一笔两件商品的交易，消耗 SKU-000001 两件库存，保留一笔 OPEN 失败购物篮。它检查并发重复完成、固定收据、单次扣减、库存不足的整篮回滚和恢复扫描。

完整隔离测试需要本地测试账号具备 CREATEDB 权限并能通过本地 socket 和 localhost 登录：

```bash
python3 checkout-service/scripts/test-saga-isolated.py
```

可用 SAGA_TEST_DB_USER / SAGA_TEST_DB_PASSWORD 配置测试账号。该脚本创建四个随机命名的临时数据库、在临时端口运行真实 JAR，最后停止服务并删除测试数据库。除上述用例外，还通过故障代理模拟“Inventory 已确认但响应丢失”，重启 Checkout 并验证 RESERVING 恢复，也验证旧版 COMMITTING 恢复；也验证 Inventory 停机后 RESERVING 的恢复。不会重置正常作业数据库。运行前需已打包 Catalog、Checkout、Inventory、Analytics 四个模块。
