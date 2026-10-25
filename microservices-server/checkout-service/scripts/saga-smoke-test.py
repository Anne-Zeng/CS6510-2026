"""真实服务联调；需要 Catalog、Checkout、Inventory。成功测试消耗 SKU-000001 两件库存。
请在没有其他客户端操作库存时运行。失败测试临时预留会在 finally 中释放。
"""
import json
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen
from urllib.error import HTTPError
checkout=sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8082'
inventory=sys.argv[2] if len(sys.argv)>2 else 'http://localhost:8084'
def call(base,method,path,body=None,expected=200):
    request=Request(base+path,method=method,
                    data=None if body is None else json.dumps(body).encode(),
                    headers={'Content-Type':'application/json'})
    try:
        with urlopen(request,timeout=30) as response:
            code,raw=response.status,response.read()
    except HTTPError as ex:
        code,raw=ex.code,ex.read()
    assert code==expected,(path,code,raw)
    return json.loads(raw) if raw else None
def stocks():
    return {r['sku']:r['currentStock'] for r in call(inventory,'GET','/inventory/low-stock?threshold=2147483647')['alerts']}
def basket(skus):
    tx=call(checkout,'POST','/transactions',{'stationId':'saga-smoke'},201)['transactionId']
    for sku in skus:
        call(checkout,'POST','/transactions/'+tx+'/items',{'sku':sku})
    return '/transactions/'+tx
before=stocks()
assert before['SKU-000001']>=3 and before['SKU-000002']>0,'Need demo stock'
path=basket(['SKU-000001','SKU-000001'])
# 两个并发完成请求必须返回同一份收据，且只扣一次库存。
with ThreadPoolExecutor(max_workers=2) as pool:
    receipts=list(pool.map(lambda _:call(checkout,'POST',path+'/complete'),range(2)))
assert receipts[0]==receipts[1],receipts
assert receipts[0]['itemCount']==2
assert call(checkout,'POST',path+'/complete')==receipts[0]
assert call(checkout,'GET',path)['status']=='COMPLETED'
call(checkout,'POST',path+'/items',{'sku':'SKU-000001'},409)
after=stocks()
assert after['SKU-000001']==before['SKU-000001']-2
assert after['SKU-000002']==before['SKU-000002']
# 临时占完第二个 SKU，让整篮预留失败，第一种商品必须回滚。
hold='saga-smoke-hold-'+str(uuid.uuid4())
holdpath='/internal/reservations/'+hold
failed=None
try:
    call(inventory,'POST',holdpath,{'items':[{'sku':'SKU-000002','quantity':after['SKU-000002']}]})
    failed=basket(['SKU-000001','SKU-000002'])
    call(checkout,'POST',failed+'/complete',expected=409)
    assert call(checkout,'GET',failed)['status']=='OPEN'
    assert stocks()['SKU-000001']==after['SKU-000001']
    # 取消后仍允许修改购物篮。
    call(checkout,'POST',failed+'/items',{'sku':'SKU-000001'})
finally:
    call(inventory,'POST',holdpath+'/release')
assert stocks()==after
print('PASS: complete, concurrent retries, one stock deduction, immutable receipt, insufficient-stock rollback, reopened basket')
print('Completed:',path)
print('Retained OPEN basket:',failed)
