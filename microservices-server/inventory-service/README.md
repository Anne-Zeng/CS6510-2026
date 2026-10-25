# Inventory：原子扣减与幂等恢复

Inventory 只访问 inventory_db。公开 GET /inventory/low-stock；内部提供 /internal/reservations/{attemptId} 的提交、查询、确认和释放。attemptId 是 Saga ID，同一购物篮取消后再次尝试使用新 ID。

新 ID 提交到单写入器队列，每批最多 64 个购物篮。单条 SQL 插入 CONFIRMED 记录、按 SKU 有序锁定并扣减库存、保存明细；任一约束失败回滚整个批次，再逐篮执行以隔离无效请求。未知商品或库存不足不允许部分扣减。相同 ID、相同规范化购物篮返回已有状态，改变内容返回 409。已存在 ID 的检查仍在事务中完成。

新扣减已经确认；confirm 为幂等操作并兼容旧版 RESERVED。release 可归还旧版预留，或为尚未存在的 ID 留下 CANCELLED 墓碑，阻止迟到请求扣减；已确认库存禁止释放。Checkout 不会因响应丢失而取消可能已经成功的扣减，而是以同一 ID 恢复完成。

新请求等待写入器时不持有普通操作许可。普通操作由 INVENTORY_MAX_IN_FLIGHT（默认 8）限制，连接池由 INVENTORY_DB_POOL（默认 8）配置。队列目前无界；Checkout 的 CHECKOUT_INVENTORY_MAX_IN_FLIGHT 限制本部署的远程调用压力，但不能替代多实例全局容量控制。数据库唯一约束和行锁是幂等与并发正确性的依据。

统一启动：`./run.sh`。独立启动需要 INVENTORY_DB_URL、INVENTORY_DB_USER、INVENTORY_DB_PASSWORD，默认端口 8084。表结构及种子位于本服务 database/。完整隔离验收：`python3 scripts/test-system.py`，覆盖库存竞争、重叠购物篮、重复请求、整篮回滚、取消墓碑和故障恢复。
