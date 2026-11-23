import json


COUNTER_QUERY = """
    SELECT MAX(value) FILTER(WHERE name='subscribers') AS subscribers,
           MAX(value) FILTER(WHERE name='sales') AS sales,
           MAX(value) FILTER(WHERE name='notifications') AS notifications,
           MAX(value) FILTER(WHERE name='notifications')-MAX(value) FILTER(WHERE name='acknowledged') AS pending,
           MAX(value) FILTER(WHERE name='send_attempts') AS send_attempts
    FROM notification_metric_totals
"""


def audit_metrics(database):
    query = """
        WITH actual AS (
            SELECT (SELECT count(*) FROM subscribers) AS subscribers,
                   (SELECT count(*) FROM sale_inbox) AS sales,
                   count(*) AS notifications,
                   count(*) FILTER(WHERE acknowledged_at IS NULL) AS pending,
                   COALESCE(sum(attempts),0) AS send_attempts FROM notification_deliveries
        ), counters AS (
    """ + COUNTER_QUERY + """
        ) SELECT json_build_object('passed',to_jsonb(actual)=to_jsonb(counters),
                   'actual',to_jsonb(actual),'counters',to_jsonb(counters)) FROM actual,counters
    """
    return json.loads(database.sql(query))


def counter_query_plan(database):
    return json.loads(database.sql('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+COUNTER_QUERY))


def delivery_io_snapshot(database):
    return json.loads(database.sql("""
        SELECT json_build_object(
            'sampled_at',clock_timestamp(),
            'tables',(SELECT json_agg(row_to_json(stats)) FROM (
                SELECT relname,n_tup_ins,n_tup_upd,n_tup_hot_upd,n_dead_tup,autovacuum_count
                FROM pg_stat_user_tables WHERE schemaname='public'
                AND relname IN ('notification_deliveries','notification_sale_items')) stats),
            'io',(SELECT json_agg(row_to_json(stats)) FROM (
                SELECT relname,heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit
                FROM pg_statio_user_tables WHERE schemaname='public'
                AND relname IN ('notification_deliveries','notification_sale_items')) stats),
            'indexes',(SELECT json_agg(indexdef) FROM pg_indexes
                WHERE schemaname='public' AND tablename='notification_deliveries'),
            'table_options',(SELECT reloptions FROM pg_class
                WHERE oid='notification_deliveries'::regclass),
            'wal',(SELECT row_to_json(stats) FROM (
                SELECT wal_records,wal_fpi,wal_bytes FROM pg_stat_wal) stats))
        """))


def storage_snapshot(database):
    return json.loads(database.sql("""
        WITH sample AS MATERIALIZED (SELECT * FROM notification_deliveries LIMIT 1024),
        sizes AS (
            SELECT avg(pg_column_size(sample)) AS compact_row_bytes,
                   avg(pg_column_size(ROW(sample.notification_id::text,item.event_id,sample.subscriber_id,
                       item.transaction_id,item.sku,item.quantity,item.completed_at,sample.acknowledged_at,
                       sample.next_attempt_at,sample.attempts))) AS legacy_equivalent_row_bytes
            FROM sample JOIN notification_sale_items item USING(item_id)
        ) SELECT json_build_object(
            'shared_items',(SELECT count(*) FROM notification_sale_items),
            'deliveries',(SELECT value FROM notification_metric_totals WHERE name='notifications'),
            'sample_limit',1024,'sample_rows',(SELECT count(*) FROM sample),
            'row_sizes',(SELECT row_to_json(sizes) FROM sizes),
            'relation_bytes',json_build_object(
                'shared_items',pg_total_relation_size('notification_sale_items'),
                'delivery_state',pg_total_relation_size('notification_deliveries')))
        """))
