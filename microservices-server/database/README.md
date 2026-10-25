# 上一版共享 schema 参考

此目录的 schema.sql 不用于当前微服务初始化。当前四套私有 schema 位于 catalog-service/database、checkout-service/database、inventory-service/database、analytics-service/database。统一初始化使用 python3 scripts/system.py init-db；它保留现有数据并应用各自的 schema/迁移。
