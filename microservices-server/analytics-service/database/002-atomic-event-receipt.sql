-- 升级时先整理旧版本已接收但未编号的事件；保留原表与全部数据。
BEGIN;
SELECT pg_advisory_xact_lock(6510,3);
WITH pending AS MATERIALIZED (
 SELECT received_id,event_key,sku,name FROM scan_inbox WHERE processed=false ORDER BY received_id FOR UPDATE
), inserted AS (
 INSERT INTO scan_events(sequence,event_key,sku,name)
 SELECT base.last_sequence+row_number() OVER(ORDER BY p.received_id),p.event_key,p.sku,p.name
 FROM pending p CROSS JOIN (SELECT COALESCE(MAX(sequence),0) AS last_sequence FROM scan_events) base
 RETURNING event_key
)
UPDATE scan_inbox inbox SET processed=true FROM inserted i WHERE inbox.event_key=i.event_key;
COMMIT;
