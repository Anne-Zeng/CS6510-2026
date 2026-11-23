BEGIN;
SELECT pg_advisory_xact_lock(6510,21);
CREATE TABLE IF NOT EXISTS subscribers (
 subscriber_id varchar(64) PRIMARY KEY,
 token_hash varchar(64) NOT NULL,
 subscribed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS subscriptions (
 subscriber_id varchar(64) NOT NULL REFERENCES subscribers(subscriber_id),
 sku varchar(32) NOT NULL,
 PRIMARY KEY(subscriber_id,sku)
);
CREATE INDEX IF NOT EXISTS subscriptions_sku_subscriber ON subscriptions(sku,subscriber_id);
DROP INDEX IF EXISTS subscriptions_sku;
CREATE TABLE IF NOT EXISTS sale_inbox (
 event_id varchar(36) PRIMARY KEY,
 transaction_id varchar(36) NOT NULL UNIQUE,
 payload jsonb NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION notification_ordered_uuid() RETURNS text
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
 identifier bytea := uuid_send(gen_random_uuid());
 timestamp_bytes bytea := int8send(floor(extract(epoch FROM clock_timestamp())*1000)::bigint);
BEGIN
 identifier := substring(timestamp_bytes FROM 3 FOR 6)||substring(identifier FROM 7);
 identifier := set_byte(identifier,6,(get_byte(identifier,6)&15)|112);
 RETURN encode(identifier,'hex')::uuid::text;
END $$;

CREATE TABLE IF NOT EXISTS notification_sale_items (
 item_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 event_id varchar(36) NOT NULL REFERENCES sale_inbox(event_id),
 transaction_id varchar(36) NOT NULL,
 sku varchar(32) NOT NULL,
 quantity integer NOT NULL CHECK(quantity>0),
 completed_at timestamptz NOT NULL,
 UNIQUE(event_id,sku)
);
CREATE TABLE IF NOT EXISTS notification_deliveries (
 notification_id uuid PRIMARY KEY DEFAULT notification_ordered_uuid()::uuid,
 item_id bigint NOT NULL REFERENCES notification_sale_items(item_id),
 subscriber_id varchar(64) NOT NULL REFERENCES subscribers(subscriber_id),
 acknowledged_at timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 attempts integer NOT NULL DEFAULT 0
);
CREATE OR REPLACE FUNCTION notification_immutable_item() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'Sale item payload is immutable'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS immutable_sale_item ON notification_sale_items;
CREATE TRIGGER immutable_sale_item BEFORE UPDATE ON notification_sale_items
 FOR EACH ROW EXECUTE FUNCTION notification_immutable_item();
LOCK TABLE subscribers,sale_inbox,notification_deliveries,notification_sale_items IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='notification_deliveries'::regclass
            AND attname='event_id' AND NOT attisdropped) THEN
  ALTER TABLE notification_deliveries ADD COLUMN item_id bigint REFERENCES notification_sale_items(item_id);
  INSERT INTO notification_sale_items(event_id,transaction_id,sku,quantity,completed_at)
  SELECT inbox.event_id,inbox.transaction_id,item.sku,item.quantity,(inbox.payload->>'completedAt')::timestamptz
  FROM sale_inbox inbox
  CROSS JOIN LATERAL jsonb_to_recordset(inbox.payload->'items') AS item(sku text,quantity integer)
  ON CONFLICT(event_id,sku) DO NOTHING;
  IF EXISTS (
   SELECT 1 FROM notification_deliveries delivery
   LEFT JOIN notification_sale_items item ON item.event_id=delivery.event_id AND item.sku=delivery.sku
   WHERE item.item_id IS NULL OR item.quantity IS DISTINCT FROM delivery.quantity
      OR item.transaction_id IS DISTINCT FROM delivery.transaction_id
      OR item.completed_at IS DISTINCT FROM delivery.completed_at
  ) THEN RAISE EXCEPTION 'Legacy delivery payload differs from durable sale; migration aborted'; END IF;
  UPDATE notification_deliveries delivery SET item_id=item.item_id
  FROM notification_sale_items item WHERE item.event_id=delivery.event_id AND item.sku=delivery.sku;
  ALTER TABLE notification_deliveries ALTER COLUMN item_id SET NOT NULL;
  ALTER TABLE notification_deliveries DROP COLUMN event_id, DROP COLUMN transaction_id,
   DROP COLUMN sku, DROP COLUMN quantity, DROP COLUMN completed_at;
 END IF;
END $$;
ALTER TABLE notification_deliveries DROP CONSTRAINT IF EXISTS notification_deliveries_event_id_subscriber_id_sku_key;
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='notification_deliveries'::regclass
            AND attname='notification_id' AND atttypid<>'uuid'::regtype) THEN
  IF EXISTS (SELECT 1 FROM notification_deliveries WHERE notification_id::uuid::text<>notification_id::text)
   THEN RAISE EXCEPTION 'Noncanonical notification identifier; migration aborted'; END IF;
  ALTER TABLE notification_deliveries ALTER COLUMN notification_id DROP DEFAULT;
  ALTER TABLE notification_deliveries ALTER COLUMN notification_id TYPE uuid USING notification_id::uuid;
 END IF;
END $$;
ALTER TABLE notification_deliveries ALTER COLUMN notification_id SET DEFAULT notification_ordered_uuid()::uuid;
ALTER TABLE notification_deliveries RESET (fillfactor);
CREATE INDEX IF NOT EXISTS deliveries_dispatch ON notification_deliveries(subscriber_id,next_attempt_at,notification_id)
 WHERE acknowledged_at IS NULL;
DROP INDEX IF EXISTS deliveries_ready;
DROP INDEX IF EXISTS deliveries_pending;

LOCK TABLE subscribers,sale_inbox,notification_deliveries IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS notification_metric_totals (
 name text PRIMARY KEY CHECK(name IN ('subscribers','sales','notifications','acknowledged','send_attempts')),
 value bigint NOT NULL CHECK(value>=0)
);
INSERT INTO notification_metric_totals(name,value)
SELECT 'subscribers',count(*) FROM subscribers
UNION ALL SELECT 'sales',count(*) FROM sale_inbox
UNION ALL SELECT 'notifications',count(*) FROM notification_deliveries
UNION ALL SELECT 'acknowledged',count(*) FROM notification_deliveries WHERE acknowledged_at IS NOT NULL
UNION ALL SELECT 'send_attempts',COALESCE(sum(attempts),0) FROM notification_deliveries
ON CONFLICT(name) DO NOTHING;

CREATE OR REPLACE FUNCTION notification_metric_add(metric_name text,delta bigint) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
 IF delta<>0 THEN
  UPDATE notification_metric_totals SET value=value+delta WHERE name=metric_name;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing notification metric %',metric_name; END IF;
 END IF;
END $$;

CREATE OR REPLACE FUNCTION notification_metric_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 PERFORM notification_metric_add(TG_ARGV[0],(SELECT count(*) FROM new_rows));
 IF TG_TABLE_NAME='notification_deliveries' THEN
  PERFORM notification_metric_add('send_attempts',(SELECT COALESCE(sum(attempts),0)::bigint FROM new_rows));
  PERFORM notification_metric_add('acknowledged',(SELECT count(*) FROM new_rows WHERE acknowledged_at IS NOT NULL));
 END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION notification_metric_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 PERFORM notification_metric_add(TG_ARGV[0],-(SELECT count(*) FROM old_rows));
 IF TG_TABLE_NAME='notification_deliveries' THEN
  PERFORM notification_metric_add('send_attempts',-(SELECT COALESCE(sum(attempts),0)::bigint FROM old_rows));
  PERFORM notification_metric_add('acknowledged',-(SELECT count(*) FROM old_rows WHERE acknowledged_at IS NOT NULL));
 END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION notification_metric_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 PERFORM notification_metric_add('send_attempts',
  (SELECT COALESCE(sum(attempts),0)::bigint FROM new_rows)-(SELECT COALESCE(sum(attempts),0)::bigint FROM old_rows));
 PERFORM notification_metric_add('acknowledged',
  (SELECT count(*) FROM new_rows WHERE acknowledged_at IS NOT NULL)-(SELECT count(*) FROM old_rows WHERE acknowledged_at IS NOT NULL));
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION notification_metric_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 UPDATE notification_metric_totals SET value=0 WHERE name=TG_ARGV[0];
 IF TG_TABLE_NAME='notification_deliveries' THEN
  UPDATE notification_metric_totals SET value=0 WHERE name IN ('send_attempts','acknowledged');
 END IF;
 RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS metric_subscribers_insert ON subscribers;
CREATE TRIGGER metric_subscribers_insert AFTER INSERT ON subscribers
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_insert('subscribers');
DROP TRIGGER IF EXISTS metric_subscribers_delete ON subscribers;
CREATE TRIGGER metric_subscribers_delete AFTER DELETE ON subscribers
 REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_delete('subscribers');
DROP TRIGGER IF EXISTS metric_subscribers_truncate ON subscribers;
CREATE TRIGGER metric_subscribers_truncate AFTER TRUNCATE ON subscribers
 FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_truncate('subscribers');
DROP TRIGGER IF EXISTS metric_sales_insert ON sale_inbox;
CREATE TRIGGER metric_sales_insert AFTER INSERT ON sale_inbox
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_insert('sales');
DROP TRIGGER IF EXISTS metric_sales_delete ON sale_inbox;
CREATE TRIGGER metric_sales_delete AFTER DELETE ON sale_inbox
 REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_delete('sales');
DROP TRIGGER IF EXISTS metric_sales_truncate ON sale_inbox;
CREATE TRIGGER metric_sales_truncate AFTER TRUNCATE ON sale_inbox
 FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_truncate('sales');
DROP TRIGGER IF EXISTS metric_deliveries_insert ON notification_deliveries;
CREATE TRIGGER metric_deliveries_insert AFTER INSERT ON notification_deliveries
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_insert('notifications');
DROP TRIGGER IF EXISTS metric_deliveries_update ON notification_deliveries;
CREATE TRIGGER metric_deliveries_update AFTER UPDATE ON notification_deliveries
 REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_update();
DROP TRIGGER IF EXISTS metric_deliveries_delete ON notification_deliveries;
CREATE TRIGGER metric_deliveries_delete AFTER DELETE ON notification_deliveries
 REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_delete('notifications');
DROP TRIGGER IF EXISTS metric_deliveries_truncate ON notification_deliveries;
CREATE TRIGGER metric_deliveries_truncate AFTER TRUNCATE ON notification_deliveries
 FOR EACH STATEMENT EXECUTE FUNCTION notification_metric_truncate('notifications');
COMMIT;
