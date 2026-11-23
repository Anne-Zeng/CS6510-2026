import argparse
import collections
from datetime import datetime
import json
from pathlib import Path
from system import databases_from, load_environment


def rows(database, query):
    return json.loads(database.sql("SELECT COALESCE(json_agg(result),'[]'::json) FROM (" + query + ") result"))


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def verify(databases, directory):
    sales = rows(databases['checkout'], """
        SELECT t.transaction_id,t.completed_at,i.sku,i.quantity
        FROM checkout_transactions t JOIN transaction_items i USING(transaction_id)
        WHERE t.status='COMPLETED'
        """)
    subscriptions = rows(databases['notification'], """
        SELECT s.subscriber_id,s.sku,u.subscribed_at
        FROM subscriptions s JOIN subscribers u USING(subscriber_id)
        """)
    sales_by_sku = collections.defaultdict(list)
    for sale in sales:
        sales_by_sku[sale['sku']].append((sale['transaction_id'], instant(sale['completed_at']), sale['quantity']))
    subscriptions_by_user = collections.defaultdict(list)
    for subscription in subscriptions:
        subscriptions_by_user[subscription['subscriber_id']].append(subscription)
    paths = {path.stem: path for path in Path(directory).glob('*.jsonl')}
    expected_count = received_count = missing_count = extra_count = wrong_count = 0
    duplicates = 0
    latencies = []
    samples = {'missing': [], 'unexpected': [], 'wrong_quantity': []}
    for subscriber in sorted(set(subscriptions_by_user) | set(paths)):
        expected = {}
        for subscription in subscriptions_by_user[subscriber]:
            subscribed_at = instant(subscription['subscribed_at'])
            for transaction_id, completed_at, quantity in sales_by_sku[subscription['sku']]:
                if completed_at >= subscribed_at:
                    expected[(transaction_id, subscriber, subscription['sku'])] = quantity
        actual = {}
        if subscriber in paths:
            with paths[subscriber].open() as lines:
                for line in lines:
                    record = json.loads(line)
                    if record['subscriberId'] != subscriber:
                        raise ValueError('Subscriber identity does not match log filename: ' + str(paths[subscriber]))
                    key = (record['transactionId'], record['subscriberId'], record['sku'])
                    duplicates += int(key in actual)
                    actual[key] = record['quantity']
                    latencies.append((instant(record['receivedAt']) - instant(record['completedAt'])).total_seconds() * 1000)
        missing = expected.keys() - actual.keys()
        extra = actual.keys() - expected.keys()
        wrong = [key for key in expected.keys() & actual.keys() if expected[key] != actual[key]]
        expected_count += len(expected)
        received_count += len(actual)
        missing_count += len(missing)
        extra_count += len(extra)
        wrong_count += len(wrong)
        for name, keys in [('missing', missing), ('unexpected', extra), ('wrong_quantity', wrong)]:
            samples[name].extend(sorted(keys)[:max(0, 5-len(samples[name]))])
    latencies.sort()
    def percentile(fraction):
        return round(latencies[min(len(latencies)-1, int((len(latencies)-1)*fraction))], 2) if latencies else None
    return {
        'passed': not (missing_count or extra_count or wrong_count or duplicates) and bool(expected_count),
        'completed_sales': len({sale['transaction_id'] for sale in sales}),
        'expected_notifications': expected_count, 'received_notifications': received_count,
        'missing': missing_count, 'unexpected': extra_count, 'wrong_quantity': wrong_count,
        'duplicate_log_entries': duplicates,
        'latency_ms': {'p50': percentile(.5), 'p95': percentile(.95), 'p99': percentile(.99)},
        'failure_samples': samples
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file')
    parser.add_argument('--logs', required=True)
    args = parser.parse_args()
    result = verify(databases_from(load_environment(args.env_file)), args.logs)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result['passed'] else 1)
