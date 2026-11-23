import collections
from datetime import datetime
import json
from pathlib import Path
import random
from verify_notifications import rows


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def elapsed(start, end):
    return (instant(end) - instant(start)).total_seconds() * 1000


def summary(values):
    values = sorted(values)
    def percentile(fraction):
        return round(values[int((len(values)-1)*fraction)], 2) if values else None
    return {'count': len(values), 'negative_count': sum(value < 0 for value in values),
            'p50': percentile(.5), 'p95': percentile(.95), 'p99': percentile(.99),
            'mean': round(sum(values)/len(values), 2) if values else None}


def ingest_timings(path):
    events = {}
    batches = []
    marker = 'NOTIFICATION_INGEST_TIMING '
    for line in Path(path).read_text().splitlines():
        if marker not in line:
            continue
        data = json.loads(line.split(marker, 1)[1])
        batches.append(elapsed(data['batchStartedAt'], data['commitObservedAt']))
        for event_id in data['eventIds']:
            events.setdefault(event_id, data)
    return events, batches


def pool_diagnostics(path):
    samples = []
    for line in Path(path).read_text().splitlines():
        if 'NOTIFICATION_POOL_TIMING ' in line:
            samples.append(json.loads(line.split('NOTIFICATION_POOL_TIMING ', 1)[1]))
    return {'samples': len(samples),
            'samples_with_waiters': sum(sample['waiting'] > 0 for sample in samples),
            'maximum_waiters': max((sample['waiting'] for sample in samples), default=None),
            'active_connections': summary([sample['active'] for sample in samples]),
            'waiting_threads': summary([sample['waiting'] for sample in samples]),
            'dispatch_gate_waiting_threads': summary([sample['dispatchWaiting'] for sample in samples if 'dispatchWaiting' in sample]),
            'note': 'Connection/thread counts, not milliseconds. One-second non-atomic snapshots across the full service run; short contention may be missed. No waiters does not rule out CPU, disk or SQL contention.'}


def diagnose(databases, run):
    run = Path(run)
    events, batches = ingest_timings(run/'services/notification.log')
    sales = rows(databases['checkout'], """
        SELECT payload->>'eventId' AS event_id,payload->>'completedAt' AS completed_at,
               publish_started_at,broker_acked_at FROM sale_outbox
        """)
    phases = collections.defaultdict(list)
    slow_plans = []
    for line in (run/'services/notification.log').read_text().splitlines():
        if 'NOTIFICATION_SLOW_ACK_PLAN ' in line:
            plan = json.loads(line.split('NOTIFICATION_SLOW_ACK_PLAN ', 1)[1])
            plan['plan'] = json.loads(plan['plan'])
            slow_plans.append(plan)
        for marker, fields in [('NOTIFICATION_DISPATCH_SQL ', ('sentMs', 'ackMs', 'stateWriteMs', 'pendingMs')),
                               ('NOTIFICATION_DISPATCH_CALL ', ('gateMs', 'transactionMs'))]:
            if marker in line:
                timing = json.loads(line.split(marker, 1)[1])
                busy = timing.get('fetched', 0) + timing.get('writes', 0) + timing.get('sentCount', 0) + timing.get('ackCount', 0)
                category = 'busy' if busy else 'idle'
                for field in fields:
                    if field in timing:
                        phases[f'dispatch_{field}_{category}'].append(timing[field])
        if 'NOTIFICATION_DB_TIMING ' not in line:
            continue
        timing = json.loads(line.split('NOTIFICATION_DB_TIMING ', 1)[1])
        category = 'new_sales' if timing['insertedSales'] else 'deduplicated'
        for field in ('lockMs', 'inboxSqlMs', 'fanoutSqlMs'):
            phases[f'database_{field}_{category}_per_chunk'].append(timing[field])
    for sale in sales:
        published = sale['publish_started_at']
        acked = sale['broker_acked_at']
        if published:
            phases['sale_to_publish_start_per_sale'].append(elapsed(sale['completed_at'], published))
        if published and acked:
            phases['publish_to_broker_ack_per_sale'].append(elapsed(published, acked))
        batch = events.get(sale['event_id'])
        if published and batch:
            received = batch.get('callbackReceivedAt', batch['batchStartedAt'])
            phases['publish_start_to_consumer_callback_per_sale'].append(elapsed(published, received))
            phases['consumer_callback_to_chunk_start_per_sale'].append(elapsed(received, batch['batchStartedAt']))
            phases['chunk_start_to_commit_observed_per_sale'].append(elapsed(batch['batchStartedAt'], batch['commitObservedAt']))
            phases['consumer_callback_to_commit_observed_per_sale'].append(elapsed(received, batch['commitObservedAt']))
    phases['consumer_transaction_per_batch'] = batches
    sample = []
    sampled_records = 0
    generator = random.Random(6510)
    for path in sorted((run/'subscribers').glob('*.jsonl')):
        with path.open() as lines:
            for line in lines:
                record = json.loads(line)
                batch = events.get(record['eventId'])
                if record.get('sentAt') and batch:
                    phases['commit_observed_to_dispatch_per_notification'].append(elapsed(batch['commitObservedAt'], record['sentAt']))
                if record.get('sentAt'):
                    phases['dispatch_to_client_record_per_notification'].append(elapsed(record['sentAt'], record['receivedAt']))
                sampled_records += 1
                if len(sample) < 2048:
                    sample.append(record)
                else:
                    index = generator.randrange(sampled_records)
                    if index < len(sample):
                        sample[index] = record
    if sample:
        identifiers = json.dumps([record['notificationId'] for record in sample]).replace("'", "''")
        acknowledgements = rows(databases['notification'], f"""
            SELECT notification_id,acknowledged_at FROM notification_deliveries
            WHERE notification_id IN (SELECT jsonb_array_elements_text('{identifiers}'::jsonb)::uuid)
            """)
        by_id = {row['notification_id']: row['acknowledged_at'] for row in acknowledgements}
        for record in sample:
            acked = by_id.get(record['notificationId'])
            if acked:
                phases['client_record_to_ack_update_sample'].append(elapsed(record['receivedAt'], acked))
    return {
        'units': 'milliseconds',
        'connection_pool': pool_diagnostics(run/'services/notification.log'),
        'slow_ack_cached_plans': slow_plans,
        'coverage': {'sales': len(sales), 'sales_with_ingest_timing': sum(sale['event_id'] in events for sale in sales),
                     'client_records': sampled_records, 'ack_sample_size': len(sample)},
        'stages': {name: summary(values) for name, values in phases.items()},
        'notes': [
            'Successful publish attempt timestamps; first saved values retained during replay.',
            'Publish-to-callback includes broker transport and Kafka waiting; broker-ack duration overlaps it.',
            'Commit observed is callback return after transactional proxy commit; includes pool wait, locking and SQL.',
            'Chunk start follows earlier chunks in the same callback; transaction duration is measured per chunk.',
            'Database SQL timings exclude transaction connection acquisition and commit; lockMs includes the advisory-lock query round trip.',
            'Database timing logs are emitted before commit and are diagnostic attempts, not proof of durability; new-sale and deduplicated chunks are separated.',
            'A concurrent dispatcher can observe commit before the callback returns; small negative timings are retained.',
            'Client record time precedes log flush and ACK. ACK timestamp is SQL update time, not exact commit time.',
            'ACK uses a seeded reservoir sample of at most 2048 client records; other notification stages use all records.',
            'Per-sale, per-batch and per-notification percentiles have different weighting and must not be added.',
            'Includes recovery probes; clocks must be synchronized across machines. This test runs locally.'
        ]}


def capture_query_plans(database, run):
    identifiers = rows(database, "SELECT notification_id FROM notification_deliveries WHERE subscriber_id='test-0' AND acknowledged_at IS NULL LIMIT 512")
    ids = json.dumps([row['notification_id'] for row in identifiers]).replace("'", "''")
    selection = f"SELECT jsonb_array_elements_text('{ids}'::jsonb)::uuid"
    queries = {
        'sent_update': f"UPDATE notification_deliveries SET attempts=attempts+1,next_attempt_at=clock_timestamp()+interval '2 seconds' WHERE notification_id IN ({selection})",
        'ack_update': f"UPDATE notification_deliveries SET acknowledged_at=COALESCE(acknowledged_at,clock_timestamp()) WHERE subscriber_id='test-0' AND notification_id IN ({selection})",
        'pending': """WITH pending AS MATERIALIZED (
            SELECT notification_id,item_id,subscriber_id,next_attempt_at FROM notification_deliveries
            WHERE subscriber_id='test-0' AND acknowledged_at IS NULL AND next_attempt_at<=now()
            ORDER BY next_attempt_at,notification_id LIMIT 512)
            SELECT pending.notification_id,pending.subscriber_id,item.event_id,item.transaction_id,
                   item.sku,item.quantity,item.completed_at
            FROM pending JOIN notification_sale_items item USING(item_id)
            ORDER BY pending.next_attempt_at,pending.notification_id"""}
    report = {'identifier_count': len(identifiers), 'note': 'UPDATE plans are EXPLAIN only; no writes performed. Captured during backlog after fixed workload.'}
    for name, query in queries.items():
        report[name] = json.loads(database.sql('EXPLAIN (FORMAT JSON) ' + query))
    report['tables'] = rows(database, "SELECT relname,n_live_tup,n_dead_tup,n_tup_upd,last_autoanalyze,last_autovacuum FROM pg_stat_user_tables")
    (Path(run)/'dispatch-query-plans.json').write_text(json.dumps(report, indent=2))
