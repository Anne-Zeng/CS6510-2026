import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import uuid
from system import ROOT, SERVICES, Cluster, Database, fetch_json, require
from verify_notifications import verify
from notification_benchmark import assess_workload
from benchmark_runtime import PostgresInstances, seed_catalog, source_hashes, original_snapshot, monitored_run
from stage_diagnostics import diagnose, capture_query_plans
from fixed_workload import run_fixed
from notification_metrics import audit_metrics, counter_query_plan, storage_snapshot, delivery_io_snapshot
from benchmark_clock import BenchmarkClock


def wait_for(check, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        require(shutil.disk_usage(ROOT).free > 1024**3, 'Less than 1 GiB free; aborting test without deleting evidence')
        if check():
            return
        time.sleep(.25)
    raise RuntimeError('Timed out waiting for ' + getattr(check, '__name__', 'condition'))


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def post(base, path, body, expected=200):
    request = Request(base + path, json.dumps(body).encode(), {'Content-Type': 'application/json'}, method='POST')
    try:
        with urlopen(request, timeout=30) as response:
            require(response.status == expected, 'Unexpected HTTP status')
            return json.load(response)
    except HTTPError as failure:
        if failure.code != expected:
            raise
        return json.loads(failure.read())


def main():
    parser = argparse.ArgumentParser(description='Isolated PostgreSQL + Kafka + 100 WebSocket subscribers; never touches existing databases.')
    parser.add_argument('--pg-bin', type=Path, default=Path('/opt/homebrew/opt/postgresql@15/bin'))
    parser.add_argument('--kafka-distribution', type=Path, default=ROOT.parent / 'event-driven-server/target/kafka-dist')
    parser.add_argument('--pg-port', type=int, default=25432)
    parser.add_argument('--kafka-port', type=int, default=29092)
    parser.add_argument('--base-port', type=int, default=18180)
    parser.add_argument('--subscribers', type=int, default=100)
    parser.add_argument('--duration', type=int, default=10)
    parser.add_argument('--stations', type=int, default=2)
    parser.add_argument('--min-items', type=int, default=1)
    parser.add_argument('--max-items', type=int, default=3)
    parser.add_argument('--drain-timeout', type=int, default=60)
    parser.add_argument('--opening-stock', type=int, default=1_000_000)
    parser.add_argument('--catalog-size', type=int, default=2000)
    parser.add_argument('--database-servers', choices=('shared', 'separate'), default='separate')
    parser.add_argument('--java-opts', default='-Xms64m -Xmx256m -XX:TieredStopAtLevel=1')
    parser.add_argument('--notification-wal-compression', choices=('off', 'pglz', 'lz4'), default='lz4')
    parser.add_argument('--variant', choices=('original', 'notification'), default='notification')
    parser.add_argument('--diagnostics', action='store_true', help='Record notification stage timings and produce stage-diagnostics.json')
    parser.add_argument('--fixed-sales', type=int, default=0, help='Use a separate deterministic fixed-count driver; original load-client remains unchanged')
    parser.add_argument('--fixed-items', type=int, default=10)
    args = parser.parse_args()
    require(0 <= args.subscribers <= 100 and args.duration > 0, 'Invalid subscriber count or duration')
    require(args.variant != 'original' or args.subscribers == 0, 'Original baseline requires --subscribers 0')
    require(1 <= args.catalog_size <= 5000, 'Invalid catalog-size')
    require(args.fixed_sales >= 0 and (not args.fixed_sales or
            (args.fixed_sales >= args.stations and 1 <= args.fixed_items <= args.catalog_size)), 'Invalid fixed workload')
    services = SERVICES if args.variant == 'notification' else tuple(name for name in SERVICES if name != 'notification')
    require(args.stations > 0 and 1 <= args.min_items <= args.max_items and args.drain_timeout > 0,
            'Require stations > 0, 1 <= min-items <= max-items, drain-timeout > 0')
    require(1 <= args.opening_stock <= 2_000_000_000, 'Invalid opening-stock')
    require(shutil.disk_usage(ROOT).free > 3 * 1024**3, 'Need at least 3 GiB free disk space')
    pg_ports = range(args.pg_port, args.pg_port + (len(services) if args.database_servers == 'separate' else 1))
    for port in [*pg_ports, args.kafka_port, args.kafka_port+1, *range(args.base_port, args.base_port+len(services)+1)]:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
    run = ROOT / 'target' / ('notification-test-' + uuid.uuid4().hex[:10])
    run.mkdir(parents=True)
    print('Run directory: ' + str(run), flush=True)
    workload_description = (f'{args.fixed_sales} fixed sales, {args.fixed_items} items per basket'
                            if args.fixed_sales else f'{args.duration}s, baskets {args.min_items}..{args.max_items}')
    print(f'Workload: {args.stations} checkout stations, {workload_description}, '
          f'{args.subscribers} notification subscribers', flush=True)
    env = dict(os.environ)
    env['PATH'] = str(args.pg_bin) + os.pathsep + env.get('PATH', '')
    env['KAFKA_BOOTSTRAP_SERVERS'] = f'127.0.0.1:{args.kafka_port}'
    env['KAFKA_TOPIC_PREFIX'] = 'notificationtest_' + uuid.uuid4().hex[:10]
    env['KAFKA_TOPIC_PARTITIONS'] = '1'
    env['EVENT_JAVA_OPTS'] = args.java_opts
    env['NOTIFICATION_STAGE_TIMING'] = str(args.diagnostics).lower()
    env['PGPASSWORD'] = ''
    java = env.get('JAVA_BIN', 'java')
    broker = subscriber = cluster = None
    postgres = PostgresInstances(args.pg_bin, run, env)
    handles = []
    benchmark_clock = BenchmarkClock(run)
    def output(name):
        handle = (run / name).open('w')
        handles.append(handle)
        return handle
    try:
        benchmark_clock.start()
        source_root = ROOT if args.variant == 'notification' else original_snapshot(
                ROOT.parent/'event-driven-server', run/'original-source')
        (run/'environment.json').write_text(json.dumps({
            'variant': args.variant, 'catalogSize': args.catalog_size, 'initialStockPerSku': args.opening_stock,
            'databaseServers': args.database_servers, 'sharedBuffersPerInstance': '32MB',
            'fsync': True, 'synchronous_commit': True, 'jvmOptions': args.java_opts,
            'fullPageWrites': True,
            'walCompressionPerInstance': {name: args.notification_wal_compression if name == 'notification'
                                          and args.database_servers == 'separate' else 'off' for name in services},
            'subscribers': args.subscribers, 'diagnosticsEnabled': args.diagnostics,
            'fixedSales': args.fixed_sales, 'fixedItems': args.fixed_items,
            'notificationSettings': {key: env.get(key, default) for key, default in {
                'NOTIFICATION_PAYLOAD_CACHE_SIZE': '0',
                'NOTIFICATION_ACK_DRAIN_AT_GATE': 'false',
                'NOTIFICATION_DISPATCH_DB_CONCURRENCY': '1', 'NOTIFICATION_DISPATCH_BATCH_SIZE': '512',
                'NOTIFICATION_INGEST_BATCH_SIZE': '16', 'NOTIFICATION_DISPATCH_CUSTOM_PLAN': 'true',
                'NOTIFICATION_COALESCE_DISPATCH_WRITES': 'true', 'NOTIFICATION_MAX_IDLE_POLL_MS': '1000'}.items()},
            'subscriptionPolicy': 'even users all SKUs; odd users one SKU',
            'sourceHashes': source_hashes(source_root),
            'clientSourceHashes': source_hashes(ROOT.parent/'load-client/src')}, indent=2))
        ports = postgres.start(services, args.pg_port, args.database_servers, args.notification_wal_compression)
        for service in services:
            name = 'pattern_' + service + '_db'
            subprocess.run(['createdb', '-h', '127.0.0.1', '-p', str(ports[service]), '-U', 'pattern_test', name], env=env, check=True)
            env[service.upper()+'_DB_URL'] = f'jdbc:postgresql://127.0.0.1:{ports[service]}/{name}'
            env[service.upper()+'_DB_USER'] = 'pattern_test'
            env[service.upper()+'_DB_PASSWORD'] = ''
        for service in ('checkout', 'notification'):
            if service not in services:
                continue
            for field in ('URL', 'USER', 'PASSWORD'):
                env[service.upper()+'_TEST_DB_'+field] = env[service.upper()+'_DB_'+field]
        subprocess.run(['mvn', '-q', 'package'], cwd=source_root, env=env, stdout=output('maven.log'), stderr=subprocess.STDOUT, check=True)
        databases = {name: Database(name, env) for name in services}
        for database in databases.values():
            database.initialize(seed=False, source_root=source_root)
        inventory_setup = seed_catalog(databases, args.catalog_size, args.opening_stock)
        (run/'inventory-setup.json').write_text(json.dumps(inventory_setup, indent=2))
        print(f'Isolated test inventory: {args.opening_stock:,} units per SKU', flush=True)
        broker = subprocess.Popen([sys.executable, str(ROOT/'scripts/kafka-local.py'),
                '--distribution', str(args.kafka_distribution), '--port', str(args.kafka_port),
                '--data-dir', str(run/'kafka'), '--heap-mb', '256'], cwd=ROOT, env=env,
                stdout=output('kafka.log'), stderr=subprocess.STDOUT)
        time.sleep(5)
        require(broker.poll() is None, 'Kafka failed; see kafka.log')
        cluster = Cluster(databases, run/'services', args.base_port, env, source_root=source_root)
        cluster.start()
        if args.subscribers == 0:
            throughput, workload = run_workload(args, run, env, cluster, output)
            if args.variant == 'notification':
                wait_for(lambda: fetch_json(f'http://127.0.0.1:{args.base_port+5}/metrics')['sales']
                         == int(databases['checkout'].sql('SELECT count(*) FROM sale_outbox')), args.drain_timeout)
            result = {'passed': workload['passed'], 'load_passed': workload['passed'],
                      'notification_passed': None, 'subscribers': 0, 'variant': args.variant,
                      'throughput': throughput, 'inventory_setup': inventory_setup}
            timing_safety = benchmark_clock.snapshot()
            (run/'clock-safety.json').write_text(json.dumps(timing_safety, indent=2))
            result['performance_valid'] = timing_safety['valid']
            result['power_conditions'] = timing_safety['power_conditions']
            (run/'verification.json').write_text(json.dumps(result, indent=2))
            print(json.dumps(result, indent=2), flush=True)
            require(result['passed'], 'Load baseline failed')
            require(result['performance_valid'], 'Baseline clock, sleep guard or power conditions invalid; rerun performance test')
            return
        wsurl = f'ws://127.0.0.1:{args.base_port+5}/notifications'
        notification_url = f'http://127.0.0.1:{args.base_port+5}'
        catalog = fetch_json(cluster.base_url + '/items')['items']
        skus = sorted(item['sku'] for item in catalog)
        logs = run/'subscribers'
        subscriber_command = [java, '-jar', str(ROOT/'subscriber-client/target/subscriber-client-0.0.1-SNAPSHOT.jar'),
                '--url', wsurl, '--subscribers', str(args.subscribers),
                '--seconds', str(args.duration + 2 * args.drain_timeout + 300),
                '--skus', ','.join(skus), '--prefix', 'test', '--output', str(logs)]
        subscriber = subprocess.Popen(subscriber_command, env=env, stdout=output('subscribers.log'), stderr=subprocess.STDOUT)
        wait_for(lambda: (logs/'ready.json').exists())
        require(subscriber.poll() is None, 'Subscriber client exited')
        def transaction(complete=True):
            started = post(cluster.base_url, '/transactions', {'stationId': 'notification-test'}, 201)['transactionId']
            for unused in range(3):
                post(cluster.base_url, f'/transactions/{started}/items', {'sku': skus[0]})
            if complete:
                post(cluster.base_url, f'/transactions/{started}/complete', {})
            return started
        open_id = transaction(False)
        require(databases['checkout'].sql('SELECT count(*) FROM sale_outbox') == '0', 'Scans must not produce sales')
        sale_id = transaction()
        post(cluster.base_url, f'/transactions/{sale_id}/complete', {})
        require(databases['checkout'].sql('SELECT count(*) FROM sale_outbox') == '1', 'Repeated complete must not duplicate sale')
        def drained():
            require(subscriber.poll() is None, 'Subscriber process exited; inspect subscribers-restarted.log/subscribers.log')
            metrics = fetch_json(notification_url+'/metrics')
            total = int(databases['checkout'].sql('SELECT count(*) FROM sale_outbox'))
            snapshot = {'sampled_at': time.time(), 'expected_sales': total,
                        'expected_connected_users': args.subscribers, **metrics}
            (run/'drain-status.json').write_text(json.dumps(snapshot, indent=2))
            return metrics['sales'] == total and metrics['pending'] == 0
        wait_for(drained)
        stop(subscriber)
        wait_for(lambda: fetch_json(notification_url+'/metrics')['connected_users'] == 0)
        transaction()
        wait_for(lambda: fetch_json(notification_url+'/metrics')['pending'] > 0)
        cluster.stop_service('notification')
        cluster.start_service('notification')
        (logs/'ready.json').unlink(missing_ok=True)
        subscriber = subprocess.Popen(subscriber_command, env=env, stdout=output('subscribers-restarted.log'), stderr=subprocess.STDOUT)
        wait_for(lambda: (logs/'ready.json').exists())
        wait_for(drained)
        started_at = time.monotonic()
        throughput, workload = run_workload(args, run, env, cluster, output)
        print('Waiting for notification backlog to drain; see /metrics on port ' + str(args.base_port+5), flush=True)
        (run/'delivery-io-after-load.json').write_text(json.dumps(delivery_io_snapshot(databases['notification']), indent=2))
        if args.fixed_sales and args.diagnostics:
            capture_query_plans(databases['notification'], run)
        wait_for(drained, args.drain_timeout)
        before = fetch_json(notification_url+'/metrics')['notifications']
        databases['checkout'].sql('UPDATE sale_outbox SET processed=false')
        wait_for(lambda: databases['checkout'].sql('SELECT count(*) FROM sale_outbox WHERE NOT processed') == '0',
                 args.drain_timeout)
        time.sleep(3)
        wait_for(drained, args.drain_timeout)
        require(fetch_json(notification_url+'/metrics')['notifications'] == before, 'Replay duplicated deliveries')
        counter_audit = audit_metrics(databases['notification'])
        (run/'metrics-audit.json').write_text(json.dumps(counter_audit, indent=2))
        (run/'metrics-query-plan.json').write_text(json.dumps(counter_query_plan(databases['notification']), indent=2))
        require(counter_audit['passed'], 'Notification counters differ from independent full-table audit')
        (run/'storage-snapshot.json').write_text(json.dumps(storage_snapshot(databases['notification']), indent=2))
        (run/'delivery-io-final.json').write_text(json.dumps(delivery_io_snapshot(databases['notification']), indent=2))
        (run/'payload-cache-metrics.json').write_text(json.dumps(fetch_json(notification_url+'/cache-metrics'), indent=2))
        result = verify(databases, logs)
        result['notification_passed'] = result['passed']
        result['load_passed'] = workload['passed']
        result['passed'] = result['notification_passed'] and result['load_passed']
        result['workload_validation'] = workload
        result['inventory_setup'] = inventory_setup
        result['metrics_audit_passed'] = counter_audit['passed']
        if args.fixed_sales:
            require(throughput['totalTransactions'] == args.fixed_sales, 'Fixed sale count mismatch')
            baskets = json.loads((run/'fixed-workload.json').read_text())['baskets']
            subscriptions = [set(skus) if index % 2 == 0 else {skus[index % len(skus)]}
                             for index in range(args.subscribers)]
            planned = sum(len(set(basket) & subscribed) for basket in baskets for subscribed in subscriptions)
            planned += 2 * sum(skus[0] in subscribed for subscribed in subscriptions)
            require(result['completed_sales'] == args.fixed_sales + 2, 'Fixed completed sales mismatch')
            require(result['expected_notifications'] == planned, 'Fixed notification count mismatch')
            result['fixed_expected_notifications'] = planned
        if args.diagnostics:
            stages = diagnose(databases, run)
            (run/'stage-diagnostics.json').write_text(json.dumps(stages, indent=2))
            print('NOTIFICATION STAGE DIAGNOSTICS:', flush=True)
            print(json.dumps(stages, indent=2), flush=True)
        result.update({'subscribers': args.subscribers, 'throughput': throughput,
                       'elapsed_load_and_drain_seconds': round(time.monotonic()-started_at, 2),
                       'metrics': fetch_json(notification_url+'/metrics'),
                       'checks': ['scan_without_sale', 'repeat_complete', 'offline_delivery', 'service_restart', 'outbox_replay',
                                  'fixed_workload' if args.fixed_sales else 'original_load_client']})
        timing_safety = benchmark_clock.snapshot()
        (run/'clock-safety.json').write_text(json.dumps(timing_safety, indent=2))
        result['performance_valid'] = timing_safety['valid']
        result['power_conditions'] = timing_safety['power_conditions']
        (run/'verification.json').write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2), flush=True)
        require(result['passed'], 'Run failed; check notification_passed, load_passed and workload-validation.json')
        require(result['performance_valid'], 'Benchmark clock, sleep guard or power conditions invalid; correctness evidence retained, performance must be rerun')
        require(subscriber.poll() is None, 'Subscriber client exited unexpectedly')
        print('Evidence: ' + str(run), flush=True)
    except Exception as failure:
        failure_report = {'passed': False, 'error': str(failure)}
        if (run/'drain-status.json').exists():
            failure_report['last_drain_status'] = json.loads((run/'drain-status.json').read_text())
        (run/'failure.json').write_text(json.dumps(failure_report, indent=2))
        raise
    finally:
        stop(subscriber)
        try:
            if cluster is not None:
                cluster.stop()
        finally:
            try:
                stop(broker)
            finally:
                try:
                    postgres.stop()
                finally:
                    benchmark_clock.close()
                    for handle in handles:
                        handle.close()
                    print('Run directory (preserved): ' + str(run), flush=True)


def run_workload(args, run, env, cluster, output):
    if args.fixed_sales:
        skus = sorted(item['sku'] for item in fetch_json(cluster.base_url+'/items')['items'])
        report = run_fixed(cluster.base_url, args.fixed_sales, args.fixed_items, args.stations,
                           skus, run, post, args.drain_timeout)
        (run/'throughput.json').write_text(json.dumps(report, indent=2))
        workload = assess_workload(report)
        (run/'workload-validation.json').write_text(json.dumps(workload, indent=2))
        print('FIXED WORKLOAD (not original load-client): ' + json.dumps(report), flush=True)
        return report, workload
    load_classes = run/'load-classes'
    load_classes.mkdir()
    subprocess.run(['javac', '--release', '21', '-d', str(load_classes),
                    *map(str, (ROOT.parent/'load-client/src').glob('*.java'))], env=env, check=True)
    monitored_run([env.get('JAVA_BIN', 'java'), '-cp', str(load_classes), 'Main', '--baseUrl='+cluster.base_url,
                   '--stations='+str(args.stations), '--duration='+str(args.duration),
                   '--minItems='+str(args.min_items), '--maxItems='+str(args.max_items),
                   '--reportDir='+str(run/'load-report')], env, output('load-client.log'), run, args.duration+90)
    reports = sorted((run/'load-report').glob('*.json'))
    require(len(reports) == 1, 'Expected one original load-client report')
    report = json.loads(reports[0].read_text())
    throughput = {key: report[key] for key in
                  ('config', 'wallClockSeconds', 'totalTransactions', 'transactionsPerSecond', 'itemsPerSecond', 'operations')}
    (run/'throughput.json').write_text(json.dumps(throughput, indent=2))
    workload = assess_workload(report)
    (run/'workload-validation.json').write_text(json.dumps(workload, indent=2))
    print('LOAD CLIENT THROUGHPUT (before notification drain/replay):', flush=True)
    print(json.dumps(throughput, indent=2), flush=True)
    print('LOAD WORKLOAD VALIDATION: ' + json.dumps(workload), flush=True)
    return throughput, workload


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    main()
