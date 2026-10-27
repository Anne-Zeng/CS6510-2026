# Self Checkout — Kafka Event Based Architecture

本版本在 `microservices-server` 的基础上保留 Catalog、Checkout、Inventory、Analytics 和 Gateway 五个独立进程。**所有服务间的业务通信都经过 Kafka**，包括 Gateway 的命令/查询、Checkout 的目录读取和库存协调，以及扫描事件。Load Client 仍通过同步 HTTP 访问 Gateway，遵守原来的 [OpenAPI 合同](../spec/self-checkout-openapi.yaml)。

![Five-service architecture with Kafka and private databases](docs/architecture.svg)

完整的拓扑、消息清单和流程图见 [architecture.md](docs/architecture.md)。`event-messaging` 是五个进程使用的共享消息库，封装原生 Apache Kafka Java clients；它没有数据库，也不是第六个微服务。本项目没有使用 Spring Kafka。

## 服务与数据边界

| 进程 | 默认 HTTP 端口 | 私有数据库示例 | 职责 |
| --- | --- | --- | --- |
| Gateway | 8080 | 无 | 同步 HTTP 入口；发送 Kafka 请求并等待业务结果 |
| Catalog | 8081 | `event_catalog_db` | 商品目录；响应 `GET_CATALOG` |
| Checkout | 8082 | `event_checkout_db` | 购物篮、收据、持久化 Saga、扫描 outbox、命令去重 |
| Analytics | 8083 | `event_analytics_db` | 扫描 inbox、事件序列、滑动窗口和排名 |
| Inventory | 8084 | `event_inventory_db` | 库存、幂等库存操作和扣减记录 |

各服务只访问自己的数据库。业务服务保留的 HTTP controllers 供本地诊断和兼容已有测试使用；运行中的服务不会通过这些接口调用彼此。启动脚本的 `/health` 探测属于本地运维检查。Gateway 不公开 `/internal/**`，也不直接转发到任何业务服务的 HTTP 端口。

## 通信与正确性

- **命令和查询**：Gateway 将原 HTTP 操作映射为 Kafka 请求，在关联的回复到达后返回相同状态码和 JSON。Checkout 使用相同机制读取 Catalog 快照、向 Inventory 发送库存命令。请求使用独立关联 ID，交易和 Saga ID 用作相关操作的分区键。
- **扫描事件**：一次成功扫描在 Checkout 的同一本地事务中保存购物篮变更和 outbox；后台将 `ItemScanned` 事件批次发布到 `<prefix>.scans`。获得 broker 确认后标记 outbox 已发布。Analytics 用事件 UUID 在自己的 inbox 去重，然后生成规范事件序列与排名。扫描成功无需等待 Analytics。
- **结账 Saga**：Checkout 是持久化协调者，冻结购物篮，经 Kafka 请求 Inventory 原子扣减整篮库存，再持久化本地完成状态与收据。常规成功路径中库存扣减即确认；兼容恢复旧 `COMMITTING` 状态。拒绝时执行取消并恢复购物篮；未知结果保留同一个 Saga ID 重试。没有跨库事务，也不声称纯事件编舞。
- **同步完成**：完成接口返回成功之前，Inventory 扣减和 Checkout 完成状态必须都已持久化。收到 broker 发布确认不等于业务完成。
- **分析查询**：Gateway 先通过 Kafka 取得 Checkout 的持久化扫描计数，再要求 Analytics 等待该检查点及应发布窗口。超过等待上限时明确失败，避免把陈旧排名当成已经追平的结果。

Kafka 与数据库之间没有分布式原子提交。消息可能重投；命令结果、Saga ID 和 inbox 事件 ID 提供幂等处理。本项目采用至少一次投递，不声称端到端 exactly-once。架构取舍见 [ADRs](docs/adr/0001-use-kafka.md)。

## 本地准备与启动

需要 Java 21+、Python 3、PostgreSQL 15+ 与 `psql`、Docker Compose，以及 Maven Wrapper 能访问的 Maven 依赖源。以下命令在 `event-driven-server/` 执行。

先启动单节点 Kafka：

```bash
docker compose up -d --wait
cp .env.example .env
```

Compose 使用 Kafka KRaft 模式、一个本地持久卷和复制因子 1，只适合本次本地作业。它不启动 PostgreSQL。以 PostgreSQL 管理员创建四套独立数据库/用户，并为每个用户设置密码，例如：

```sql
CREATE ROLE event_catalog_user LOGIN PASSWORD 'choose-a-catalog-password';
CREATE DATABASE event_catalog_db OWNER event_catalog_user;
CREATE ROLE event_checkout_user LOGIN PASSWORD 'choose-a-checkout-password';
CREATE DATABASE event_checkout_db OWNER event_checkout_user;
CREATE ROLE event_inventory_user LOGIN PASSWORD 'choose-an-inventory-password';
CREATE DATABASE event_inventory_db OWNER event_inventory_user;
CREATE ROLE event_analytics_user LOGIN PASSWORD 'choose-an-analytics-password';
CREATE DATABASE event_analytics_db OWNER event_analytics_user;
```

编辑 `.env`，填入四套 `*_DB_URL`、`*_DB_USER`、`*_DB_PASSWORD`。使用新的 `event_*` 数据库保留上一版数据。`.env` 被 Git 忽略；脚本按文本解析它，不执行 shell。不要提交密码。

```bash
./run.sh
```

脚本构建模块、初始化各自 schema、仅为空的商品/库存表准备演示数据，然后启动四个业务服务和 Gateway。启动过程会检查端口及就绪状态。已有交易和库存不会因再次启动而重置。日志位于 `target/runtime/`。Ctrl+C 关闭本次启动的五个 Java 进程，数据库与 Kafka 持久数据保留。

```bash
# 已完成构建时：
./run.sh --skip-build
# 使用另一组五个连续的 HTTP 监听端口：
./run.sh --skip-build --base-port=18090
# 仅初始化已有数据库：
python3 scripts/system.py init-db
```

停止 Kafka 使用 `docker compose down`；普通停止保留 Kafka 卷。更改 topic 前缀可以隔离实验消息，但不能替代隔离实验数据库。

## 主要配置

| 环境变量 | 用途 / 默认值 |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 地址，默认 `127.0.0.1:9092` |
| `KAFKA_TOPIC_PREFIX` | Topic 命名空间，默认 `selfcheckout` |
| `KAFKA_REQUEST_TIMEOUT_MS` | 一次 Kafka 请求/业务回复的等待预算，默认 `8000` |
| `KAFKA_TOPIC_PARTITIONS` | 新建命令 topic 的分区数，默认 `24` |
| `KAFKA_CHECKOUT_CONCURRENCY` / `KAFKA_INVENTORY_CONCURRENCY` | 命令消费者数量，默认 `24` / `4` |
| `KAFKA_CATALOG_CONCURRENCY` / `KAFKA_ANALYTICS_CONCURRENCY` | 命令消费者数量，各默认 `1` |
| `KAFKA_CONSUMER_CONCURRENCY` | 可选的统一并发覆盖值；服务专属设置优先 |
| `ANALYTICS_QUERY_CATCH_UP_MS` | 分析查询等待扫描及窗口追平的上限，默认 `6000` |
| `EVENT_JAVA_OPTS` | 启动脚本传递给每个 JVM 的参数 |
| `CHECKOUT_INVENTORY_MAX_IN_FLIGHT` | Checkout 同时等待的库存命令上限 |
| `INVENTORY_MAX_IN_FLIGHT` | Inventory 操作并发上限 |

实际部署值以 `.env.example`、启动参数和压测的环境记录为准。Topic 创建参数不会自动修改已有 topic。每个服务的回复 topic 和扫描事件 topic 使用一个分区；Kafka 只保证单个分区内的记录顺序。

Gateway 单独启动示例：

```bash
java -jar gateway/target/gateway-0.0.1-SNAPSHOT.jar --port=8080
```

Gateway 不再接受上一版的 `--catalog-url` 等 HTTP 上游参数。业务结果错误原样返回；Gateway 的消息传输失败为 502，等待超时为 504。

## 验证与作业提交

```bash
./mvnw package
curl http://localhost:8080/health
curl http://localhost:8080/items
curl 'http://localhost:8080/inventory/low-stock?threshold=1001'
curl 'http://localhost:8080/analytics/popular-items?limit=10'
```

81 个组件测试已通过，覆盖消息边界、HTTP 合同、Saga 和 inbox/outbox。`python3 scripts/test-events.py` 已通过真实 Kafka 与四个临时数据库的集成验收，验证事件重放去重、Analytics 停机追赶，以及 Inventory 停机和 Checkout 重启恢复。证据见 [validation.json](reports/submission/validation.json)。

正式压测使用原始 Load Client，不修改客户端业务逻辑：

```bash
python3 scripts/benchmark-isolated.py --publish-submission
```

该命令分别执行默认参数（10 stations / 60 秒）及 `--stations=100 --duration=120`，使用临时数据库和隔离的 Kafka topic 命名空间。正式负载准备 2,000 个 SKU、每 SKU 1,000,000 件起始库存；普通 `run.sh` 的小型演示种子不等同于该数据集。测试账号需具备本地数据库创建/清理能力，可配置 `SAGA_TEST_DB_USER`、`SAGA_TEST_DB_PASSWORD`。它不会重置正常运行的四套数据库；只在两组审计均通过后更新正式提交目录。

提交清单见 [SUBMISSION.md](SUBMISSION.md)，实测结果和限制见 [PERFORMANCE.md](PERFORMANCE.md)。没有经过本版本实际执行的报告不能作为 Kafka 版本的成绩证据。
