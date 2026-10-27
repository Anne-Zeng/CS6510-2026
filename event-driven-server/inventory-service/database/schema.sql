-- 功能：仅在 inventory_db 执行；没有指向 Catalog 或 Checkout 的跨库外键。
BEGIN;
CREATE TABLE IF NOT EXISTS inventory (
 sku varchar(32) PRIMARY KEY,
 name varchar(255) NOT NULL,
 current_stock integer NOT NULL CHECK(current_stock>=0)
);
CREATE TABLE IF NOT EXISTS reservations (
 transaction_id varchar(100) PRIMARY KEY,
 status varchar(16) NOT NULL CHECK(status IN ('NEW','RESERVED','CONFIRMED','CANCELLED'))
);
CREATE TABLE IF NOT EXISTS reservation_items (
 transaction_id varchar(100) REFERENCES reservations(transaction_id),
 sku varchar(32) REFERENCES inventory(sku),
 quantity integer NOT NULL CHECK(quantity>0),
 PRIMARY KEY(transaction_id,sku)
);
COMMIT;
