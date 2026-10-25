"""阶段一联调：需要 Catalog 与 Checkout 已启动；保留一笔 OPEN 演示交易。"""
import json
import sys
from decimal import Decimal
from urllib.request import Request, urlopen
from urllib.error import HTTPError
checkout = sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8082'
catalog = sys.argv[2] if len(sys.argv)>2 else 'http://localhost:8081'
def call(base, method, path, body=None, expected=200):
    req=Request(base+path, method=method,
                data=None if body is None else json.dumps(body).encode(),
                headers={'Content-Type':'application/json'})
    try:
        with urlopen(req, timeout=10) as response:
            code, raw=response.status,response.read()
    except HTTPError as ex:
        code,raw=ex.code,ex.read()
    assert code==expected,(path,code,raw)
    return json.loads(raw,parse_float=Decimal) if raw else None
item=call(catalog,'GET','/items')['items'][0]
tx=call(checkout,'POST','/transactions',{'stationId':'checkout-smoke'},201)
path='/transactions/'+tx['transactionId']
for count in (1,2):
    result=call(checkout,'POST',path+'/items',{'sku':item['sku']})
    assert result['itemCount']==count
    assert result['unitPrice']==item['price']
    assert result['runningTotal']==item['price']*count
call(checkout,'POST',path+'/items',{'sku':'UNKNOWN-SMOKE-SKU'},404)
result=call(checkout,'GET',path)
assert result['status']=='OPEN' and result['itemCount']==2
assert result['runningTotal']==item['price']*2
print('PASS: remote catalog, create, scan, exact totals, unknown SKU')
print('Retained OPEN transaction:',tx['transactionId'])
