-- 功能：仅在 Catalog 私有数据库执行。
BEGIN;
CREATE TABLE IF NOT EXISTS catalog_items (
 sku varchar(32) PRIMARY KEY,
 name varchar(255) NOT NULL,
 price numeric(10,2) NOT NULL CHECK (price >= 0)
);
COMMIT;
