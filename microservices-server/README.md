# Self Checkout — Microservices Architecture

本作业包含四个可独立部署的业务服务和一个 HTTP Gateway。每个业务服务只连接自己的 PostgreSQL 数据库，服务间通过 HTTP 协作。Java 包名保留 servicebased 以减少与上一版的无关改动。

| 进程 | 默认端口 | 私有数据库 | 公开 API |
| --- | --- | --- | --- |
| Gateway | 8080 | 无 | 统一客户端入口 |
| Catalog | 8081 | catalog_db | GET /items |
| Checkout | 8082 | checkout_db | /transactions/** |
| Analytics | 8083 | analytics_db | GET /analytics/popular-items |
| Inventory | 8084 | inventory_db | GET /inventory/low-stock |

Gateway 的 /health 只说明入口进程存活。统一脚本会先检查四个业务服务各自的 /health，再启动 Gateway，并通过 Gateway 检查三个查询路由。内部库存预留和事件接收接口不通过 Gateway 转发。

## 架构和数据一致性

- 扫描：Checkout 通过 Catalog API 首次加载不可变目录快照，后续扫描在本地 Map 查 SKU；将名称和价格快照与购物篮保存到 checkout_db。目录变更需重启 Catalog 和 Checkout，与上一版固定目录假设一致。
- 结账：Checkout 是持久化 Saga 协调者，冻结购物篮，调用 Inventory 原子扣减并确认整篮库存，再在 Checkout 本地完成交易。兼容恢复旧版 COMMITTING 状态。库存不足时取消预留并恢复购物篮；超时保留进度重试。每次尝试使用独立 saga_id，重复成功请求返回原收据。
- 扫描：并行执行单条原子 SQL，每个成功响应都等待购物篮与 outbox 完成持久化。
- 分析：Checkout 在扫描的同一本地事务写 outbox，后台通过 HTTP 批量投递。Analytics 在自己的 inbox 去重、持久化并确认；接收成功后才标记源事件已投递。窗口和排名为最终一致。
- 四套数据库之间没有外键或 SQL JOIN，也没有通过共享 Repository 访问其他服务数据。

具体流程、故障处理与独立启动方式见 [Checkout](checkout-service/README.md) 和 [Analytics](analytics-service/README.md)。

## 准备数据库和配置

需要 Java 21+、PostgreSQL 15+、psql、Python 3、Maven Wrapper。数据库和账号只需准备一次。如果已按分步指导创建过，直接使用现有的四套数据库。

首次安装时，以 PostgreSQL 管理员执行，分别设置密码：

```sql
CREATE ROLE catalog_user LOGIN PASSWORD 'catalog-password';
CREATE DATABASE catalog_db OWNER catalog_user;
CREATE ROLE checkout_user LOGIN PASSWORD 'checkout-password';
CREATE DATABASE checkout_db OWNER checkout_user;
CREATE ROLE inventory_user LOGIN PASSWORD 'inventory-password';
CREATE DATABASE inventory_db OWNER inventory_user;
CREATE ROLE analytics_user LOGIN PASSWORD 'analytics-password';
CREATE DATABASE analytics_db OWNER analytics_user;
```

在 microservices-server 目录复制配置模板并填写四个密码：

```bash
cp -n .env.example .env
```

正式压测通过命令行显式设置 JVM 和并发参数；普通启动的 `.env.example` 使用较低内存配置，两者不能视为相同性能配置。

`.env` 已被 Git 忽略；不要提交密码。脚本把配置作为文本解析，不执行 shell、不展开 `$变量`。含空格或 `#` 的密码请用引号包围。默认读取本目录 `.env`，也支持 `--env-file /absolute/path/to/config`；配置文件中的值覆盖终端变量。

脚本使用每个服务自己的账号建表和升级，不需要应用账号拥有 CREATEDB 权限。连接 URL 仅支持 `jdbc:postgresql://host:port/database`，密码放在独立配置项中。脚本拒绝将多个服务配置到同一个数据库。

## 统一启动、停止和重启

先停止分步指导期间手动启动的四个服务，避免端口冲突和并行使用同一套数据库。

```bash
./run.sh
```

该命令先检查五个端口，再构建模块、运行组件测试、初始化/升级各服务表结构；仅在 Catalog 或 Inventory 表为空时填入 20 个演示 SKU 和每 SKU 1000 件库存。随后启动四个业务服务，等待就绪，再启动 Gateway。已有交易、库存、事件与排名不会被重置，也不会自动补充已经消耗的库存。

已经构建过可以跳过构建：

```bash
./run.sh --skip-build
```

看到 `All services ready: http://127.0.0.1:8080` 后，客户端统一访问 8080。日志位于 target/runtime/catalog.log、checkout.log、inventory.log、analytics.log、gateway.log。

按 Ctrl+C 或向脚本发送 SIGTERM，会关闭它管理的五个进程，保留数据库内容。再次运行相同命令即可恢复；脚本不会查找或终止其他终端手动启动的 Java 进程。任何子进程意外退出时，脚本报告日志位置并清理本次启动的其他进程。

其他选项：

```bash
# 仅初始化或升级已有的四个数据库。
python3 scripts/system.py init-db
# 使用另一组五个连续端口：18090–18094；服务间地址自动对应更新。
./run.sh --skip-build --base-port=18090
# 自定义配置和日志目录。
./run.sh --skip-build --env-file /path/to/config.env --log-dir /path/to/logs
```

`--no-seed` 可禁止初始化演示商品与库存，供已有自定义数据使用。`--skip-init` 仅适用于已经应用当前 schema/迁移的数据库。不要在同一套数据库上并行启动多个管理集群。

## 通过 Gateway 验证

```bash
curl http://localhost:8080/health
curl http://localhost:8080/items
curl 'http://localhost:8080/inventory/low-stock?threshold=1001'
curl http://localhost:8080/analytics/popular-items
```

结账与分析冒烟脚本均可指定 Gateway 地址：

```bash
python3 checkout-service/scripts/saga-smoke-test.py http://localhost:8080 http://localhost:8084
python3 analytics-service/scripts/smoke-test.py http://localhost:8080 http://localhost:8080 http://localhost:8080
```

Saga 脚本临时占用库存时直接使用 Inventory 内部接口，因此第二个参数应为 Inventory 地址。它会真实消耗 SKU-000001 两件库存。Analytics 脚本创建 OPEN 购物篮并扫描 500 次，不扣库存。请在没有其他客户端操作时运行。

## 构建和隔离验收

```bash
./mvnw package
python3 scripts/test-system.py
```

Maven 执行 43 个组件测试。隔离验收使用四个临时数据库和临时端口启动真实服务，验证 Saga、Analytics 积压/去重/排名、Gateway 路由，以及真实 run.sh 的初始化、端口冲突、停止清理和重启保留数据。测试结束会清理测试进程与临时数据库，不重置普通作业数据库。

隔离验收的测试账号需具有 CREATEDB 权限并能通过本地 socket 和 localhost 登录。默认使用操作系统用户名；可以设置 SAGA_TEST_DB_USER / SAGA_TEST_DB_PASSWORD。测试使用同一管理员账号创建四个临时测试库；正常运行使用 .env 中四个独立服务账号。

Gateway 可单独部署，支持 --port、--catalog-url、--checkout-url、--analytics-url、--inventory-url 参数。公开路由保持原有 load-client 合同；连接失败返回 502，超时返回 504，内部接口和未知路径返回 404。

## 当前范围和历史文件

本次已实现四个私有数据存储、跨库结账协调、可靠扫描事件投递、Gateway 和统一运行入口。shared-persistence/ 与根目录 database/schema.sql 是上一版源码参考，不再参与构建或初始化；当前 schema 位于各服务自己的 database/ 下。

正式负载结果见 [PERFORMANCE.md](PERFORMANCE.md)，提交说明见 [SUBMISSION.md](SUBMISSION.md)。Default 为 183.74 TPS，Stress 为 261.69 TPS，两组均零请求错误且通过 17 项跨库审计。相比用户上次提交，Default 下降 17.53%，Stress 提高 13.71%。临时压测目录保留在本地，正式证据位于 reports/submission；reports/comparison 中的旧数据仅为历史参考。
