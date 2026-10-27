#!/usr/bin/env python3
"""Exercise real Kafka delivery, isolated PostgreSQL databases, Gateway routes and crash recovery.

Build the JARs and start Kafka before running. Only randomly named temporary databases
are created or dropped. SAGA_TEST_DB_USER/PASSWORD select a CREATEDB test account;
the default account is the current operating-system user. No HTTP fault proxy is used.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import getpass
import json
import os
from pathlib import Path
import random
import re
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from system import Cluster, Database, ROOT, SERVICES, require


def available_base_port():
    """Probe all five ports together, avoiding a collision within the chosen range."""
    for _ in range(100):
        base = random.randrange(20000, 55000)
        sockets = []
        try:
            for offset in range(5):
                probe = socket.socket()
                sockets.append(probe)
                probe.bind(('127.0.0.1', base + offset))
            return base
        except OSError:
            pass
        finally:
            for probe in sockets:
                probe.close()
    raise RuntimeError('Could not find five free local ports')


def http(base, method, path, body=None, expected=200, timeout=25):
    raw = body.encode() if isinstance(body, str) else None if body is None else json.dumps(body).encode()
    request = Request(base + path, method=method, data=raw,
                      headers={'Content-Type': 'application/json', 'Connection': 'close'})
    try:
        with urlopen(request, timeout=timeout) as response:
            status, payload = response.status, response.read()
    except HTTPError as error:
        status, payload = error.code, error.read()
    result = json.loads(payload) if payload else None
    if status >= 400:
        require(isinstance(result, dict) and isinstance(result.get('error'), str)
                and isinstance(result.get('message'), str), 'Error response violates OpenAPI ApiError fields')
    allowed = {expected} if isinstance(expected, int) else set(expected)
    require(status in allowed, f'{method} {path}: expected {sorted(allowed)}, got {status}: {result}')
    return status, result


def wait_for(description, predicate, timeout=60, interval=.2):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        require(time.monotonic() < deadline, 'Timed out: ' + description)
        time.sleep(interval)


def rows(database, query):
    text = database.sql('SELECT COALESCE(json_agg(result),\'[]\'::json) FROM (' + query + ') result')
    return json.loads(text)


def static_transport_check():
    forbidden = re.compile(r'\b(?:HttpClient|RestClient|RestTemplate|WebClient|HttpURLConnection|URL\.openConnection)\b')
    failures = []
    scanned = 0
    for module in ('gateway', *(name + '-service' for name in SERVICES)):
        for source in (ROOT / module / 'src/main/java').rglob('*.java'):
            scanned += 1
            if forbidden.search(source.read_text()):
                failures.append(str(source.relative_to(ROOT)))
    require(not failures, 'Outbound HTTP client found in production modules: ' + ', '.join(failures))
    return {'javaFilesInspected': scanned, 'outboundHttpClients': failures}


def audit(databases):
    """Recompute cross-service facts independently, joining data in Python rather than SQL."""
    checkout, inventory, analytics = (databases[name] for name in ('checkout', 'inventory', 'analytics'))
    transactions = rows(checkout, 'SELECT transaction_id,status,item_count,running_total FROM checkout_transactions')
    lines = rows(checkout, 'SELECT transaction_id,sku,quantity,unit_price FROM transaction_items')
    outbox = rows(checkout, 'SELECT event_key,sku,item_name,processed FROM scan_outbox ORDER BY event_id')
    scans = rows(analytics, 'SELECT sequence,event_key,sku,name FROM scan_events ORDER BY sequence')
    stocks = rows(inventory, 'SELECT sku,current_stock FROM inventory ORDER BY sku')
    sold = Counter()
    for transaction in transactions:
        items = [line for line in lines if line['transaction_id'] == transaction['transaction_id']]
        require(transaction['status'] == 'COMPLETED', 'A test basket did not complete')
        require(sum(line['quantity'] for line in items) == transaction['item_count'], 'Basket count mismatch')
        total = sum((Decimal(str(line['unit_price'])) * line['quantity'] for line in items), Decimal(0))
        require(total == Decimal(str(transaction['running_total'])), 'Basket total mismatch')
        sold.update({line['sku']: line['quantity'] for line in items})
    for stock in stocks:
        require(stock['current_stock'] >= 0, 'Negative inventory')
        require(10000 - stock['current_stock'] == sold[stock['sku']], 'Stock differs from completed sales')
    original = {event['event_key']: (event['sku'], event['item_name']) for event in outbox}
    received = {event['event_key']: (event['sku'], event['name']) for event in scans}
    require(len(original) == len(outbox) == sum(sold.values()), 'Outbox count differs from accepted scans')
    require(len(received) == len(scans) and original == received, 'Analytics lost or duplicated event UUIDs/payloads')
    require(all(event['processed'] for event in outbox), 'Unpublished outbox remains')
    require([event['sequence'] for event in scans] == list(range(1, len(scans) + 1)), 'Canonical sequence has gaps')
    require(int(analytics.sql('SELECT count(*) FROM scan_inbox')) == len(outbox), 'Inbox dedup count differs')
    saved_windows = rows(analytics, 'SELECT id,window_size,slide_interval,window_start,window_end FROM analytics_windows ORDER BY window_end')
    require([window['window_end'] for window in saved_windows] == list(range(500, len(scans) + 1, 500)),
            'Missing or extra ranking window')
    for window in saved_windows:
        first, last = max(1, window['window_end'] - 999), window['window_end']
        require((window['window_size'], window['slide_interval'], window['window_start']) == (1000, 500, first),
                'Incorrect window metadata')
        counts = Counter(event['sku'] for event in scans[first - 1:last])
        expected = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ranking = rows(analytics, f'SELECT sku,scan_count,rank FROM popular_item_results WHERE window_id={window["id"]} ORDER BY rank')
        require([(item['sku'], item['scan_count']) for item in ranking] == expected, 'Incorrect persisted ranking')
        require([item['rank'] for item in ranking] == list(range(1, len(ranking) + 1)), 'Ranking positions have gaps')
    require(int(checkout.sql("SELECT count(*) FROM checkout_sagas WHERE state NOT IN ('DONE','ABORTED')")) == 0,
            'Saga recovery left active work')
    return {'transactions': len(transactions), 'acceptedScans': len(outbox), 'uniqueAnalyticsEvents': len(scans),
            'rankingWindows': len(saved_windows), 'stockMatchesCompletedSales': True,
            'basketTotalsMatchLines': True, 'eventUuidPayloadSetsMatch': True, 'activeSagas': 0}


def run_checks(cluster, databases, record):
    base = cluster.base_url
    checkout, inventory, analytics = (databases[name] for name in ('checkout', 'inventory', 'analytics'))

    def call(method, path, body=None, expected=200):
        return http(base, method, path, body, expected)[1]

    def stock(sku):
        return int(inventory.sql("SELECT current_stock FROM inventory WHERE sku='" + sku + "'"))

    def start(station):
        return call('POST', '/transactions', {'stationId': station}, 201)['transactionId']

    def scan(transaction, sku):
        return call('POST', f'/transactions/{transaction}/items', {'sku': sku})

    require(call('GET', '/health')['status'] == 'UP', 'Gateway health')
    catalog = call('GET', '/items')['items']
    require(len(catalog) == 20, 'Expected twenty seeded catalog items')
    call('POST', '/transactions', {}, 400)
    call('POST', '/transactions', '{invalid', 400)
    call('GET', '/transactions/' + str(uuid.uuid4()), expected=404)
    call('DELETE', '/items', expected=405)
    call('GET', '/does-not-exist', expected=404)
    call('GET', '/inventory/low-stock?threshold=-1', expected=400)
    call('GET', '/inventory/low-stock?threshold=invalid', expected=400)
    call('GET', '/analytics/popular-items?limit=0', expected=400)
    call('GET', '/analytics/popular-items?limit=invalid', expected=400)
    require(not call('GET', '/inventory/low-stock')['alerts'], 'Initial low-stock alerts should be empty')
    require(len(call('GET', '/inventory/low-stock?threshold=10001')['alerts']) == 20, 'Custom threshold not forwarded')
    require(not call('GET', '/analytics/popular-items')['items'], 'Fresh analytics should be empty')
    record('gateway_routes_and_validation', True)

    transaction = start('integration-normal')
    path = f'/transactions/{transaction}'
    require(call('GET', path)['status'] == 'OPEN', 'New basket should be OPEN')
    call('POST', path + '/complete', expected=409)
    call('POST', path + '/items', {'sku': ''}, 400)
    call('POST', path + '/items', {'sku': 'UNKNOWN'}, 404)
    scan(transaction, 'SKU-000001')
    second = scan(transaction, 'SKU-000001')
    require(second['itemCount'] == 2 and stock('SKU-000001') == 10000, 'A scan deducted stock')
    with ThreadPoolExecutor(max_workers=6) as executor:
        receipts = list(executor.map(lambda _: call('POST', path + '/complete'), range(12)))
    require(all(receipt == receipts[0] for receipt in receipts), 'Repeated complete returned different receipts')
    require(receipts[0]['itemCount'] == 2 and stock('SKU-000001') == 9998, 'Repeated complete debited more than once')
    require(int(inventory.sql('SELECT count(*) FROM reservations')) == 1, 'Repeated complete created duplicate reservations')
    call('POST', path + '/items', {'sku': 'SKU-000001'}, 409)
    call('GET', '/analytics/popular-items?limit=1')
    require(int(analytics.sql('SELECT count(*) FROM scan_events')) == 2, 'Initial scans not ingested')
    record('scan_is_non_debiting_and_concurrent_complete_is_idempotent', {'completeRequests': 12, 'stockDebited': 2})

    cluster.stop_service('analytics')
    backlog = start('integration-analytics-outage')
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: scan(backlog, 'SKU-000002'), range(500)))
    require(sorted(result['itemCount'] for result in results) == list(range(1, 501)), 'Concurrent scans lost basket updates')
    require(stock('SKU-000002') == 10000, 'Backlogged analytics scans deducted inventory')
    receipt = call('POST', f'/transactions/{backlog}/complete')
    require(receipt['itemCount'] == 500 and stock('SKU-000002') == 9500, 'Checkout depends on Analytics availability')
    wait_for('Kafka broker acknowledges outbox while Analytics is absent',
             lambda: checkout.sql('SELECT count(*) FROM scan_outbox WHERE NOT processed') == '0')
    require(int(analytics.sql('SELECT count(*) FROM scan_events')) == 2, 'Stopped Analytics unexpectedly ingested events')
    # Simulate a publisher crash after Kafka ack but before its local marker update.
    # The genuine publisher sends the exact same durable UUIDs again through Kafka.
    replayed = int(checkout.sql('SELECT count(*) FROM scan_outbox'))
    checkout.sql('UPDATE scan_outbox SET processed=false')
    wait_for('genuine outbox publisher replays duplicate UUIDs through Kafka',
             lambda: checkout.sql('SELECT count(*) FROM scan_outbox WHERE NOT processed') == '0')
    cluster.start_service('analytics')
    # The scans topic has one partition. This fresh event is published after all
    # replayed records; observing it proves the consumer passed every duplicate.
    recovery = start('integration-inventory-checkout-recovery')
    scan(recovery, 'SKU-000003')
    ranking = call('GET', '/analytics/popular-items')
    require(ranking['windowEnd'] == 500, 'Final query did not await its due ranking window')
    require(int(analytics.sql('SELECT count(*) FROM scan_events')) == 503, 'Checkpoint query omitted acknowledged scans')
    require(int(analytics.sql('SELECT count(*) FROM scan_inbox')) == 503, 'Kafka replay duplicated inbox events')
    record('analytics_outage_durable_kafka_delivery_and_uuid_dedup',
           {'scansWhileOffline': 500, 'replayedEventUuids': replayed, 'uniqueEventsAfterRestart': 503,
            'freshEventAfterReplayWasConsumed': True, 'queryWindowEnd': ranking['windowEnd']})

    cluster.stop_service('inventory')
    status, _ = http(base, 'POST', f'/transactions/{recovery}/complete', expected={502, 503, 504})
    require(stock('SKU-000003') == 10000, 'Unavailable Inventory somehow debited stock')
    require(call('GET', f'/transactions/{recovery}')['status'] == 'CHECKOUT_PENDING', 'Unknown completion was reported as final')
    saga = checkout.sql("SELECT saga_id FROM checkout_sagas WHERE transaction_id='" + recovery + "'")
    require(re.fullmatch(r'[0-9a-f-]{36}', saga), 'Missing persistent recovery Saga')
    cluster.stop_service('checkout')
    cluster.start_service('inventory')
    wait_for('Inventory consumes command persisted while it was offline',
             lambda: inventory.sql("SELECT count(*) FROM reservations WHERE transaction_id='" + saga + "' AND status='CONFIRMED'") == '1')
    require(stock('SKU-000003') == 9999, 'Queued Inventory command did not debit exactly once')
    cluster.start_service('checkout')
    wait_for('restarted Checkout recovers the same persistent Saga',
             lambda: checkout.sql("SELECT status FROM checkout_transactions WHERE transaction_id='" + recovery + "'") == 'COMPLETED',
             timeout=90)
    recovered_receipt = call('POST', f'/transactions/{recovery}/complete')
    require(recovered_receipt['itemCount'] == 1 and stock('SKU-000003') == 9999,
            'Checkout restart repeated inventory debit')
    require(inventory.sql("SELECT count(*) FROM reservations WHERE transaction_id='" + saga + "'") == '1',
            'Recovery used a second reservation ID')
    call('GET', '/analytics/popular-items')
    record('inventory_outage_and_checkout_restart_recover_without_false_success',
           {'unavailableHttpStatus': status, 'queuedInventoryCommandRecovered': True, 'stockDebited': 1})
    return audit(databases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='Directory for validation.json and service logs')
    parser.add_argument('--base-port', type=int, help='Five consecutive ports; default selects a free range')
    args = parser.parse_args()
    suffix = uuid.uuid4().hex[:12]
    output = (args.output or ROOT / 'reports' / ('integration-' + datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S') + '-' + suffix[:6])).resolve()
    output.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment['KAFKA_BOOTSTRAP_SERVERS'] = environment.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
    environment['KAFKA_TOPIC_PREFIX'] = 'integration_' + suffix
    environment.setdefault('EVENT_JAVA_OPTS', '-Xms32m -Xmx192m -XX:TieredStopAtLevel=1')
    environment.setdefault('CHECKOUT_SAGA_RECOVERY_MS', '1000')
    environment.setdefault('CHECKOUT_OUTBOX_POLL_MS', '50')
    user = environment.get('SAGA_TEST_DB_USER', getpass.getuser())
    password = environment.get('SAGA_TEST_DB_PASSWORD', '')
    host = environment.get('SAGA_TEST_DB_HOST', '127.0.0.1')
    port = int(environment.get('SAGA_TEST_DB_PORT', '5432'))
    for service in SERVICES:
        environment.update({service.upper() + '_DB_URL': f'jdbc:postgresql://{host}:{port}/event_test_{service}_{suffix}',
                            service.upper() + '_DB_USER': user, service.upper() + '_DB_PASSWORD': password})
    databases = {service: Database(service, environment) for service in SERVICES}
    admin_environment = {**environment, 'CATALOG_DB_URL': f'jdbc:postgresql://{host}:{port}/postgres'}
    admin = Database('catalog', admin_environment)
    cluster = Cluster(databases, output, args.base_port or available_base_port(), environment)
    created = []
    checks = []
    result = {'success': False, 'startedAt': datetime.now(timezone.utc).isoformat(),
              'kafkaTopicPrefix': environment['KAFKA_TOPIC_PREFIX'], 'checks': checks,
              'temporaryDatabasesRemoved': False}

    def record(name, details):
        checks.append({'name': name, 'passed': True, 'details': details})
        print('PASS: ' + name, flush=True)

    failure = None
    try:
        record('no_outbound_http_clients', static_transport_check())
        cluster.check_ports()
        cluster.check_jars()
        for service, database in databases.items():
            admin.sql('CREATE DATABASE ' + database.name)
            created.append(database.name)
            database.initialize(seed=False)
            if service == 'catalog':
                database.sql("INSERT INTO catalog_items(sku,name,price) SELECT 'SKU-'||lpad(n::text,6,'0'),'Item '||n,1.00+n/100.0 FROM generate_series(1,20) n")
            elif service == 'inventory':
                database.sql("INSERT INTO inventory(sku,name,current_stock) SELECT 'SKU-'||lpad(n::text,6,'0'),'Item '||n,10000 FROM generate_series(1,20) n")
        cluster.start()
        result['audit'] = run_checks(cluster, databases, record)
        result['success'] = True
    except Exception as error:
        failure = error
        result['error'] = str(error)
    finally:
        cleanup_errors = []
        try:
            cluster.stop()
        except Exception as error:
            cleanup_errors.append(str(error))
        for name in reversed(created):
            try:
                require(re.fullmatch(r'event_test_[a-z]+_' + suffix, name), 'Refusing to drop an unrecognized database')
                admin.sql('DROP DATABASE ' + name + ' WITH (FORCE)')
            except Exception as error:
                cleanup_errors.append(str(error))
        result['temporaryDatabasesRemoved'] = not cleanup_errors
        if cleanup_errors:
            result['cleanupErrors'] = cleanup_errors
            result['success'] = False
        result['finishedAt'] = datetime.now(timezone.utc).isoformat()
        (output / 'validation.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        print('Validation report: ' + str(output / 'validation.json'), flush=True)
    if failure:
        raise failure
    require(result['success'], 'Integration cleanup failed; inspect validation.json')
    print('PASS: real Kafka + PostgreSQL integration and recovery checks', flush=True)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        sys.exit(1)
