"""通过真实 Checkout 扫描 500 次，等待 Analytics 发布新窗口。
保留 OPEN 购物篮；扫描不扣库存。请在无其他客户端运行时执行。
"""
import json
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
checkout=sys.argv[1] if len(sys.argv)>1 else 'http://localhost:8082'
analytics=sys.argv[2] if len(sys.argv)>2 else 'http://localhost:8083'
catalog=sys.argv[3] if len(sys.argv)>3 else 'http://localhost:8081'
def call(base,method,path,body=None,expected=200):
    req=Request(base+path,method=method,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    try:
        with urlopen(req,timeout=15) as response:code,raw=response.status,response.read()
    except HTTPError as error:code,raw=error.code,error.read()
    assert code==expected,(path,code,raw)
    return json.loads(raw) if raw else None
before=call(analytics,'GET','/analytics/popular-items')['windowEnd']
item=call(catalog,'GET','/items')['items'][0]
tx=call(checkout,'POST','/transactions',{'stationId':'analytics-smoke'},201)['transactionId']
for n in range(500):
    call(checkout,'POST','/transactions/'+tx+'/items',{'sku':item['sku']})
    if (n+1)%100==0:print('Scans:',n+1,flush=True)
deadline=time.monotonic()+60
while True:
    result=call(analytics,'GET','/analytics/popular-items')
    if result['windowEnd']>=before+500:break
    assert time.monotonic()<deadline,'No new window: check outbox delivery and Analytics logs'
    time.sleep(.25)
assert result['windowSize']==1000 and result['slideInterval']==500
assert result['windowStart']==max(1,result['windowEnd']-999)
assert 1<=len(result['items'])<=10
assert [(r['sku'],r['scanCount']) for r in result['items']]==sorted([(r['sku'],r['scanCount']) for r in result['items']],key=lambda r:(-r[1],r[0]))
for rank,row in enumerate(result['items'],1):
    assert row['rank']==rank and row['name'] and row['scanCount']>0
print('PASS: Checkout scans -> outbox -> HTTP inbox -> published ranking')
print('Window:',result['windowStart'],result['windowEnd'])
print('Retained OPEN transaction:',tx)
print(json.dumps(result['items'],ensure_ascii=False,indent=2))
