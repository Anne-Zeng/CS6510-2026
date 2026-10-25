"""在独立启动的演示 Inventory 上验证；临时预留在 finally 中释放。"""
import json
import sys
import uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError

base = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:8084'

def call(method, path, body=None, expected=200):
    data = None if body is None else json.dumps(body).encode()
    req = Request(base + path, data=data, method=method,
                  headers={'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=10) as response:
            status, raw = response.status, response.read()
    except HTTPError as error:
        status, raw = error.code, error.read()
    assert status == expected, (path, status, raw)
    return json.loads(raw) if raw else None

def stock():
    rows = call('GET', '/inventory/low-stock?threshold=2147483647')['alerts']
    return {row['sku']: row['currentStock'] for row in rows}

prefix = '/internal/reservations/'
ids = ['smoke-' + str(uuid.uuid4()) for _ in range(3)]
original = stock()
assert original.get('SKU-000001', 0) >= 2, 'Need at least two units of SKU-000001'
body = {'items': [{'sku': 'SKU-000001', 'quantity': 2}]}
try:
    assert call('POST', prefix + ids[0], body)['status'] == 'RESERVED'
    call('POST', prefix + ids[0], body)
    assert stock()['SKU-000001'] == original['SKU-000001'] - 2
    call('POST', prefix + ids[0], {'items': [{'sku': 'SKU-000001', 'quantity': 1}]}, 409)
    call('POST', prefix + ids[0] + '/release')
    call('POST', prefix + ids[0] + '/release')
    assert stock() == original
    # 先释放再收到延迟预留，不能重新扣库存。
    call('POST', prefix + ids[1] + '/release')
    call('POST', prefix + ids[1], body, 409)
    # 第一行可扣，第二行不足，必须连第一行也回滚。
    call('POST', prefix + ids[2], {'items': [
        {'sku': 'SKU-000001', 'quantity': 1},
        {'sku': 'SKU-000002', 'quantity': 2147483647}]}, 409)
    assert stock() == original
    call('POST', prefix + ids[0] + '/confirm', expected=409)
    print('PASS: retries, payload mismatch, release, cancellation, basket rollback')
finally:
    for ident in ids:
        call('POST', prefix + ident + '/release')
