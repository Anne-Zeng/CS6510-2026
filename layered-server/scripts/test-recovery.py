#!/usr/bin/env python3
"""Isolated integration checks: receipts, rollback, backlog, retry and restart.
Injected 500 responses are expected fault checks, not benchmark request errors.
"""
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import urllib.error
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
DB = 'layered_acceptance_recovery'
PORT = 18083
ENV = {**os.environ, 'PGPASSWORD': os.environ.get('CHECKOUT_DB_PASSWORD', 'postgres')}
OUT = ROOT / 'reports' / ('recovery-' + time.strftime('%Y%m%d-%H%M%S'))
OUT.mkdir(parents=True)


def sql(query, db=DB):
    return subprocess.check_output(['psql', '-h', 'localhost', '-U', 'postgres', '-d', db,
                                   '-qtA', '-v', 'ON_ERROR_STOP=1', '-c', query], env=ENV, text=True).strip()


def api(path, data=None):
    req = urllib.request.Request(f'http://localhost:{PORT}{path}',
        data=None if data is None else json.dumps(data).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as res:
        return json.loads(res.read(), parse_float=Decimal)


def until(check):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if check(): return
        except (OSError, ValueError): pass
        time.sleep(.2)
    raise AssertionError('Timed out waiting for expected state')


server = None
logs = []
def start():
    global server
    log = open(OUT / f'server-{len(logs)}.log', 'w')
    logs.append(log)
    server = subprocess.Popen(['java', '-jar', str(ROOT / 'target/layered-server-0.0.1-SNAPSHOT.jar'),
        f'--server.port={PORT}', f'--spring.datasource.url=jdbc:postgresql://localhost:5432/{DB}',
        '--checkout.stock-per-item=1000000'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    until(lambda: len(api('/items')['items']) == 2000)


def stop():
    global server
    if server:
        server.terminate()
        server.wait(timeout=45)
        server = None


try:
    if sql(f"SELECT count(*) FROM pg_database WHERE datname='{DB}'", 'postgres') == '0':
        sql(f'CREATE DATABASE {DB}', 'postgres')
    sql('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')
    start()
    # Block ranking persistence while scans continue. No window may be acknowledged.
    sql('ALTER TABLE analytics_windows ADD CONSTRAINT injected_publish_failure CHECK (window_end < 0)')
    prices = {i['sku']: i['price'] for i in api('/items')['items']}

    def station(number):
        tx = api('/transactions', {'stationId': f'recovery-{number}'})['transactionId']
        total = Decimal(0)
        for i in range(15):
            sku = f'SKU-{1 + (number + i) % 20:06d}'
            total += prices[sku]
            scan = api(f'/transactions/{tx}/items', {'sku': sku})
            assert scan['itemCount'] == i + 1
            assert scan['runningTotal'] == total
        receipt = api(f'/transactions/{tx}/complete', {})
        assert receipt['itemCount'] == 15
        assert receipt['totalAmount'] == total
        assert sum(line['quantity'] for line in receipt['lines']) == 15
        assert sum(line['quantity'] * line['unitPrice'] for line in receipt['lines']) == total
        return total

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        amounts = list(pool.map(station, range(100)))
    assert sql('SELECT count(*) FROM scan_events') == '1500'
    assert sql('SELECT count(*) FROM analytics_windows') == '0'
    stop()
    start()
    sql('ALTER TABLE analytics_windows DROP CONSTRAINT injected_publish_failure')
    until(lambda: api('/analytics/popular-items')['windowEnd'] == 1500)
    assert sql('SELECT string_agg(window_end::text,\',\' ORDER BY window_end) FROM analytics_windows') == '500,1000,1500'
    # Compare every persisted top ten with independently computed SQL rankings.
    mismatch = sql('''WITH expected AS (
      SELECT w.id AS window_id, e.sku, count(*) AS scan_count,
        row_number() OVER (PARTITION BY w.id ORDER BY count(*) DESC, e.sku) AS rank
      FROM analytics_windows w JOIN scan_events e ON e.sequence BETWEEN w.window_start AND w.window_end
      GROUP BY w.id, e.sku
    ), differences AS (
      (SELECT window_id,sku,scan_count,rank FROM expected WHERE rank<=10
       EXCEPT SELECT window_id,sku,scan_count,rank FROM popular_item_results)
      UNION ALL
      (SELECT window_id,sku,scan_count,rank FROM popular_item_results
       EXCEPT SELECT window_id,sku,scan_count,rank FROM expected WHERE rank<=10)
    ) SELECT count(*) FROM differences''')
    assert mismatch == '0'
    before = api('/analytics/popular-items')
    stop()
    start()
    assert api('/analytics/popular-items') == before
    assert sql('SELECT count(*) FROM analytics_windows') == '3'

    # A failed analytics append must roll back the basket and leave no sequence gap.
    tx = api('/transactions', {'stationId': 'rollback'})['transactionId']
    sql("ALTER TABLE scan_events ADD CONSTRAINT injected_scan_failure CHECK (sku <> 'SKU-000001') NOT VALID")
    try:
        api(f'/transactions/{tx}/items', {'sku': 'SKU-000001'})
        raise AssertionError('Injected write failure unexpectedly succeeded')
    except urllib.error.HTTPError as error:
        assert error.code == 500
    assert api(f'/transactions/{tx}')['itemCount'] == 0
    assert sql('SELECT count(*) FROM scan_events') == '1500'
    sql('ALTER TABLE scan_events DROP CONSTRAINT injected_scan_failure')
    api(f'/transactions/{tx}/items', {'sku': 'SKU-000001'})
    api(f'/transactions/{tx}/complete', {})
    assert sql('SELECT max(sequence) FROM scan_events') == '1501'
    assert sql('SELECT count(*) FROM checkout_transactions WHERE status=\'COMPLETED\'') == '101'
    assert Decimal(sql('SELECT sum(running_total) FROM checkout_transactions')) == sum(amounts) + prices['SKU-000001']
    assert sql('''WITH sold AS (SELECT sku,sum(quantity) AS n FROM transaction_items GROUP BY sku)
      SELECT count(*) FROM inventory i LEFT JOIN sold s ON s.sku=i.sku
      WHERE 1000000-i.current_stock <> coalesce(s.n,0)''') == '0'
    result = {'passed': True, 'concurrentStations': 100, 'receiptsChecked': 100,
      'scanResponsesChecked': 1500, 'exactWindowsRecoveredAfterRestart': [500,1000,1500],
      'rankingMismatches': 0, 'publicationFailureRecovered': True,
      'atomicScanRollbackVerified': True, 'restartRankingPreserved': True,
      'inventoryMismatches': 0, 'completedTransactions': 101}
    (OUT / 'correctness.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
    print(OUT)
finally:
    stop()
    for log in logs: log.close()
