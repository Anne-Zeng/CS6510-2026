from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import shutil
import threading
import time


def plan_sales(count, items, skus):
    if count < 1 or items < 1 or items > len(skus) or len(set(skus)) != len(skus):
        raise ValueError('Require positive sales, unique catalog and 1 <= items <= catalog size')
    ordered = sorted(skus)
    return [[ordered[(sale * items + item) % len(ordered)] for item in range(items)]
            for sale in range(count)]


def run_fixed(base, count, items, stations, skus, directory, post, timeout=600):
    plan = plan_sales(count, items, skus)
    if not 1 <= stations <= count:
        raise ValueError('Require 1 <= stations <= fixed sales')
    manifest = {'sales': count, 'items_per_sale': items, 'stations': stations,
                'plan_sha256': hashlib.sha256(json.dumps(plan, separators=(',', ':')).encode()).hexdigest(),
                'baskets': plan, 'driver': 'fixed-workload-v1 (not the original load-client)'}
    (directory/'fixed-workload.json').write_text(json.dumps(manifest, indent=2))
    barrier = threading.Barrier(stations + 1)
    deadline = time.monotonic() + timeout

    def station(index):
        timings = {name: [] for name in ('START_TRANSACTION', 'SCAN_ITEM', 'COMPLETE_TRANSACTION')}

        def request(operation, path, body, expected=200):
            started = time.perf_counter()
            result = post(base, path, body, expected)
            timings[operation].append((time.perf_counter() - started) * 1000)
            return result

        barrier.wait(timeout=30)
        for basket in plan[index::stations]:
            if time.monotonic() > deadline:
                raise RuntimeError('Fixed workload timed out')
            if shutil.disk_usage(directory).free < 1024**3:
                raise RuntimeError('Less than 1 GiB free; fixed workload stopped')
            identifier = request('START_TRANSACTION', '/transactions', {'stationId': f'fixed-{index}'}, 201)['transactionId']
            for sku in basket:
                request('SCAN_ITEM', f'/transactions/{identifier}/items', {'sku': sku})
            request('COMPLETE_TRANSACTION', f'/transactions/{identifier}/complete', {})
        return timings

    with ThreadPoolExecutor(max_workers=stations) as workers:
        futures = [workers.submit(station, index) for index in range(stations)]
        started = time.perf_counter()
        barrier.wait(timeout=30)
        results = [future.result() for future in futures]
        elapsed = time.perf_counter() - started
    operations = []
    for name in results[0]:
        values = sorted(value for result in results for value in result[name])
        operations.append({'operation': name, 'successCount': len(values), 'errorCount': 0,
                           'meanMs': sum(values)/len(values),
                           'p50Ms': values[int((len(values)-1)*.5)],
                           'p95Ms': values[int((len(values)-1)*.95)],
                           'p99Ms': values[int((len(values)-1)*.99)]})
    return {'config': {key: value for key, value in manifest.items() if key != 'baskets'},
            'wallClockSeconds': elapsed, 'totalTransactions': count,
            'transactionsPerSecond': count/elapsed, 'itemsPerSecond': count*items/elapsed,
            'operations': operations}
