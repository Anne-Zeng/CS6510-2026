# Analytics：私有数据库与事件投递

Analytics 只连接 analytics_db，拥有 scan_inbox、scan_events、analytics_windows、popular_item_results。实体、Repository、HTTP 契约和异常处理均在本服务内，不依赖 shared-persistence，也不访问 Catalog 或 Checkout 的数据库。

## 数据流与一致性

1. Checkout 在同一个本地事务中保存购物篮更新和 scan_outbox。事件包含稳定 UUID、SKU 与扫描使用的商品名称快照。
2. Checkout 后台投递器每批选择最多 500 条未确认事件；有积压时连续发送最多十批，空闲时等待 250 ms，避免每轮只能发送一批导致吞吐被限制。HTTP 连接复用，通过 `POST /internal/events/scans` 发送。网络调用不占用 Checkout 的数据库事务。它使用独立调度线程，Analytics 故障不阻塞 Saga 恢复。
3. Analytics 在一个本地事务中接收整批事件，同时保存已处理 inbox 和连续 scan_events，省去新事件的第二次编号提交。接收过程使用一次事务级锁和一次集合式 SQL，批量插入并验证全部内容，100 条事件由原先 201 次应用 SQL 调用减为 2 次。event_key 唯一；相同 ID、相同内容重复接收会确认，相同 ID、不同内容返回 409，并回滚整个批次。
4. 只有确认响应列出本批所有事件 ID 后，Checkout 才用一次集合式 UPDATE 将整批事件标记已投递；不再逐条更新。若确认响应丢失、Checkout 崩溃或本地确认失败，事件会重发。
5. 新事件已经在接收事务中取得连续序号，现有三段流水线直接计算和保存排名。升级时的旧 pending inbox 由幂等迁移预先整理；旧的后台整理方法仍能恢复遗留积压，使用与接收端相同的编号锁。

这是至少一次投递加幂等处理，并非网络层恰好一次。扫描成功保证购物篮和源事件已保存，排名为最终一致。已扫描但最终没有完成购买的商品也计入扫描统计，与上一版语义一致。

分析顺序为已提交接收批次的次序及批内顺序。Checkout 的事件 IDENTITY 可有空洞，较小 ID 的事务也可能较晚提交；发送器每次重查未处理事件，不按最大已发送 ID 跳过旧事件。重复接收会消耗 inbox identity 序号，但本地 canonical scan_events 用行号生成连续序号，因此窗口不会产生缺口。并发扫描的全局精确提交顺序不作保证。

商品名称来自事件快照。排名使用窗口截止序号之前，该 SKU 最近一次扫描的名称。Analytics 不需要通过跨库查询补充名称。

## 窗口

- 窗口大小 1000，步长 500，前十名，次数相同时按 SKU 排序。
- 首次在序号 500 发布部分窗口 1–500；之后为 1–1000、501–1500 等。
- 不足一个步长的末尾事件仍持久化，但不发布额外窗口。
- 窗口元数据、商品名称查询和全部排名在一条集合式 SQL 中原子提交；窗口唯一键保证重试幂等，重启后从最后窗口恢复。

## 本地启动

在 PostgreSQL 管理会话中创建专用角色与数据库，密码自行设置：

```sql
CREATE ROLE analytics_user LOGIN PASSWORD 'your-password';
CREATE DATABASE analytics_db OWNER analytics_user;
```

在 microservices-server 目录执行：

```bash
psql -h localhost -U analytics_user -d analytics_db -v ON_ERROR_STOP=1 \
  -f analytics-service/database/schema.sql
export ANALYTICS_DB_PASSWORD='your-password'
java -jar analytics-service/target/analytics-service-0.0.1-SNAPSHOT.jar
```

默认端口 8083；可通过 ANALYTICS_PORT、ANALYTICS_DB_URL、ANALYTICS_DB_USER 修改。无需商品种子数据。

升级到原子接收版本时，先停止旧 Analytics，保留数据库并执行 `analytics-service/database/002-atomic-event-receipt.sql`，再启动新 JAR。`run.sh` 会在启动前自动应用当前 schema（已包含此迁移）。

停止旧 Checkout，保留已有数据库，升级 outbox：

```bash
psql -h localhost -U checkout_user -d checkout_db -v ON_ERROR_STOP=1 \
  -f checkout-service/database/003-event-delivery.sql
export ANALYTICS_BASE_URL=http://localhost:8083
java -jar checkout-service/target/checkout-service-0.0.1-SNAPSHOT.jar
```

需保留原来的 Checkout、Catalog、Inventory 环境变量。升级为旧事件补持久化 UUID，并从已有购物篮还原名称；上一阶段商品目录固定，名称不会变更。若某条历史事件无法还原名称，迁移会失败并回滚，需检查数据，不会静默丢弃事件。

`GET /analytics/popular-items` 在不足 500 条时返回空排名是正常行为。

## 验证

```bash
./mvnw -pl checkout-service,analytics-service -am package
python3 analytics-service/scripts/smoke-test.py
```

冒烟脚本需要 Catalog、Checkout、Analytics 已启动，创建一笔 OPEN 购物篮并扫描 500 次，等待新窗口；不完成交易、不扣库存。请在没有其他客户端时运行。

完整隔离集成测试：

```bash
python3 checkout-service/scripts/test-saga-isolated.py
```

此脚本现使用四个临时数据库和临时端口，运行真实服务后清理。除了结账回归外，还检查 Analytics 停机期间扫描与结账成功、Checkout 重启保留积压、确认丢失后的重发去重、源/目标全部事件与内容一致、序号无缺口、窗口排名正确、冲突批次回滚、Analytics 重启后排名不变。测试账号需具有 CREATEDB 权限和本地登录能力；可用 SAGA_TEST_DB_USER、SAGA_TEST_DB_PASSWORD 指定。

当前版本保留已投递 outbox 和已处理 inbox 作为审计记录，没有自动清理策略。若永久无效事件阻塞一个批次，需要修复根因；当前记录重试日志，没有死信队列。数据库被删除、历史被重置不属于自动故障恢复范围。
