-- 功能：保留已扫描事件；为 HTTP 重试增加稳定 UUID 与名称快照。
BEGIN;
ALTER TABLE scan_outbox ADD COLUMN IF NOT EXISTS event_key varchar(36) NOT NULL DEFAULT gen_random_uuid()::text;
ALTER TABLE scan_outbox ADD COLUMN IF NOT EXISTS item_name varchar(255);
UPDATE scan_outbox o SET item_name=names.item_name
FROM (SELECT DISTINCT ON (sku) sku,item_name FROM transaction_items ORDER BY sku,id DESC) names
WHERE o.sku=names.sku AND o.item_name IS NULL;
-- 功能：存在无法还原名称的旧事件时明确失败，不跳过或删除事件。
ALTER TABLE scan_outbox ALTER COLUMN item_name SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS scan_outbox_event_key ON scan_outbox(event_key);
CREATE INDEX IF NOT EXISTS scan_outbox_pending ON scan_outbox(event_id) WHERE processed=false;
COMMIT;
