"""创建并清理四个临时 PostgreSQL 数据库，启动真实 JAR 验证 Saga。
要求本机 PostgreSQL 允许当前用户通过 socket 和 localhost 登录，且有 CREATEDB 权限。
可用 SAGA_TEST_DB_USER/SAGA_TEST_DB_PASSWORD 指定测试账号。不会重置作业数据库。
"""
import getpass
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

root=Path(__file__).resolve().parents[2]
user=os.environ.get('SAGA_TEST_DB_USER',getpass.getuser())
password=os.environ.get('SAGA_TEST_DB_PASSWORD','')
suffix=uuid.uuid4().hex[:12]
databases={name:'saga_test_'+name+'_'+suffix for name in ('catalog','checkout','inventory','analytics')}
created=[]
processes={}
logs=[]
work=Path(tempfile.mkdtemp(prefix='checkout-saga-test-'))
def sql(db,statement=None,file=None):
    args=['psql','-X','-v','ON_ERROR_STOP=1','-U',user,'-d',db]
    args+=['-f',str(file)] if file else ['-Atqc',statement]
    result=subprocess.run(args,env={**os.environ,'PGPASSWORD':password},capture_output=True,text=True)
    if result.returncode: raise RuntimeError(result.stderr)
    return result.stdout.strip()
def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));return s.getsockname()[1]
ports={name:port() for name in databases}
urls={name:'http://127.0.0.1:'+str(value) for name,value in ports.items()}
def call(base,method,path,body=None,expected=200):
    req=Request(base+path,method=method,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    try:
        with urlopen(req,timeout=15) as response: code,raw=response.status,response.read()
    except HTTPError as error: code,raw=error.code,error.read()
    assert code==expected,(method,path,code,raw)
    return json.loads(raw) if raw else None
# 功能：确认已在库存提交，但故意向 Checkout 返回 503，模拟确认响应丢失。
class Proxy(BaseHTTPRequestHandler):
    lose_confirm=False
    lose_ack=False
    ack_losses=0
    pause_confirm=False
    confirm_committed=threading.Event()
    confirm_resume=threading.Event()
    def do_POST(self):
        if self.headers.get('Transfer-Encoding','').lower()=='chunked':
            parts=[]
            while True:
                size=int(self.rfile.readline().split(b';',1)[0],16)
                if not size:
                    while self.rfile.readline().strip(): pass
                    break
                parts.append(self.rfile.read(size));self.rfile.read(2)
            body=b''.join(parts)
        else:
            body=self.rfile.read(int(self.headers.get('Content-Length','0')))
        target='analytics' if self.path=='/internal/events/scans' else 'inventory'
        req=Request(urls[target]+self.path,data=body,method='POST',headers={'Content-Type':'application/json'})
        try:
            with urlopen(req,timeout=10) as response: code,raw=response.status,response.read()
        except HTTPError as error: code,raw=error.code,error.read()
        except URLError: code,raw=502,b'{}'
        inventory_commit=target=='inventory' and code==200 and (
            self.path.endswith('/confirm') or
            (self.path.startswith('/internal/reservations/') and self.path.count('/')==3))
        if inventory_commit and Proxy.pause_confirm:
            Proxy.pause_confirm=False;Proxy.confirm_committed.set();Proxy.confirm_resume.wait(2)
        if inventory_commit and Proxy.lose_confirm:
            Proxy.lose_confirm=False;code=503;raw=b'{"error":"simulated lost inventory commit response"}'
        if self.path=='/internal/events/scans' and code==200 and Proxy.lose_ack:
            Proxy.lose_ack=False;Proxy.ack_losses+=1;code=503;raw=b'{}'
        self.send_response(code);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(raw)
    def log_message(self,*args): pass
proxy=ThreadingHTTPServer(('127.0.0.1',0),Proxy)
threading.Thread(target=proxy.serve_forever,daemon=True).start()
def start(name):
    env={**os.environ,
         name.upper()+'_DB_URL':'jdbc:postgresql://localhost:5432/'+databases[name],
         name.upper()+'_DB_USER':user,name.upper()+'_DB_PASSWORD':password,
         name.upper()+'_PORT':str(ports[name]),
         'CATALOG_BASE_URL':urls['catalog'],
         'INVENTORY_BASE_URL':'http://127.0.0.1:'+str(proxy.server_port),
         'ANALYTICS_BASE_URL':'http://127.0.0.1:'+str(proxy.server_port)}
    log=open(work/(name+'-'+str(len(logs))+'.log'),'w');logs.append(log)
    args=['java','-Xms32m','-Xmx160m','-XX:TieredStopAtLevel=1','-jar',str(root/(name+'-service')/'target'/(name+'-service-0.0.1-SNAPSHOT.jar'))]
    # 功能：恢复任务启动时立即运行一次；之后延迟，便于准确观察故障后的持久化状态。
    if name=='checkout': args+=['--checkout.saga.recovery-ms=600000']
    processes[name]=subprocess.Popen(args,env=env,stdout=log,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        if processes[name].poll() is not None: raise RuntimeError(name+' exited; logs: '+str(work))
        try:
            if call(urls[name],'GET','/health')['status']=='UP':return
        except (URLError,AssertionError,OSError):pass
        time.sleep(.25)
    raise RuntimeError(name+' startup timeout; logs: '+str(work))
def stop(name):
    process=processes.pop(name,None)
    if process is not None:
        process.terminate()
        try:process.wait(10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
try:
    for name,db in databases.items():
        sql('postgres','CREATE DATABASE '+db);created.append(db)
        sql(db,file=root/(name+'-service')/'database/schema.sql')
        if name in ('catalog','inventory'): sql(db,file=root/(name+'-service')/'database/seed.sql')
    # 功能：验证已有阶段一数据库也能无损重复执行迁移。
    sql(databases['checkout'],file=root/'checkout-service/database/002-checkout-saga.sql')
    # 验证升级将旧 pending inbox 一次编号，不跳过、不删除；重复迁移不产生重复事件。
    legacy=str(uuid.uuid4())
    sql(databases['analytics'],"INSERT INTO scan_inbox(event_key,sku,name) VALUES ('"+legacy+"','LEGACY','Legacy event')")
    for _ in range(2):sql(databases['analytics'],file=root/'analytics-service/database/002-atomic-event-receipt.sql')
    assert sql(databases['analytics'],"SELECT count(*) FROM scan_events WHERE sequence=1 AND event_key='"+legacy+"'")=='1'
    assert sql(databases['analytics'],"SELECT count(*) FROM scan_inbox WHERE processed=false")=='0'
    sql(databases['analytics'],"DELETE FROM scan_events WHERE event_key='"+legacy+"'; DELETE FROM scan_inbox WHERE event_key='"+legacy+"'")
    print('PASS: legacy pending inbox migration preserves data and is idempotent',flush=True)
    for name in ('catalog','inventory','analytics','checkout'):start(name)
    subprocess.run([sys.executable,str(root/'checkout-service/scripts/saga-smoke-test.py'),urls['checkout'],urls['inventory']],check=True)
    # 功能：扫描采用单语句隐式事务；故意使 outbox 插入失败，篮子明细和汇总也必须回滚。
    atomic=call(urls['checkout'],'POST','/transactions',{'stationId':'atomic-scan'},201)['transactionId']
    atomic_path='/transactions/'+atomic
    good=call(urls['checkout'],'POST','/transactions',{'stationId':'constraint-isolation'},201)['transactionId']
    good_path='/transactions/'+good
    sql(databases['checkout'],"ALTER TABLE scan_outbox ADD CONSTRAINT injected_outbox_failure CHECK(sku<>'SKU-000018')")
    try:
        from concurrent.futures import ThreadPoolExecutor
        def injected_scan(n):
            if n%2:return call(urls['checkout'],'POST',good_path+'/items',{'sku':'SKU-000019'})
            return call(urls['checkout'],'POST',atomic_path+'/items',{'sku':'SKU-000018'},500)
        with ThreadPoolExecutor(max_workers=32) as executor:list(executor.map(injected_scan,range(32)))
        assert call(urls['checkout'],'GET',good_path)['itemCount']==16
        assert call(urls['checkout'],'GET',atomic_path)['itemCount']==0
        assert sql(databases['checkout'],"SELECT count(*) FROM transaction_items WHERE transaction_id='"+atomic+"'")=='0'
        assert sql(databases['checkout'],"SELECT count(*) FROM scan_outbox WHERE sku='SKU-000018'")=='0'
    finally:sql(databases['checkout'],"ALTER TABLE scan_outbox DROP CONSTRAINT injected_outbox_failure")
    call(urls['checkout'],'POST',good_path+'/complete')
    call(urls['checkout'],'POST',atomic_path+'/items',{'sku':'SKU-000018'})
    call(urls['checkout'],'POST',atomic_path+'/complete')
    call(urls['checkout'],'POST',atomic_path+'/items',{'sku':'SKU-000018'},409)
    assert call(urls['checkout'],'GET',atomic_path)['itemCount']==1
    print('PASS: single-statement scan rolls back basket and outbox together; closed basket rejects scan',flush=True)
    # 同一购物篮并发扫描必须逐项返回精确累计值，且并发 404/409 不影响其他合法请求。
    from concurrent.futures import ThreadPoolExecutor
    concurrent=call(urls['checkout'],'POST','/transactions',{'stationId':'concurrent-scan'},201)['transactionId']
    concurrent_path='/transactions/'+concurrent
    def scan_concurrent(n):
        if n==0:return call(urls['checkout'],'POST',atomic_path+'/items',{'sku':'SKU-000018'},409)
        if n==1:return call(urls['checkout'],'POST','/transactions/'+str(uuid.uuid4())+'/items',{'sku':'SKU-000018'},404)
        return call(urls['checkout'],'POST',concurrent_path+'/items',{'sku':'SKU-000018' if n%2 else 'SKU-000019'})
    with ThreadPoolExecutor(max_workers=24) as executor:responses=list(executor.map(scan_concurrent,range(66)))
    responses=sorted(responses[2:],key=lambda r:r['itemCount'])
    from decimal import Decimal
    subtotal=Decimal('0')
    for number,response in enumerate(responses,1):
        subtotal+=Decimal(str(response['unitPrice']))
        assert response['itemCount']==number and Decimal(str(response['runningTotal']))==subtotal
    assert call(urls['checkout'],'GET',concurrent_path)['itemCount']==64
    concurrent_receipt=call(urls['checkout'],'POST',concurrent_path+'/complete')
    assert concurrent_receipt['itemCount']==64 and Decimal(str(concurrent_receipt['totalAmount']))==subtotal
    print('PASS: concurrent same-basket exact totals and mixed 404/409 isolation',flush=True)

    # 功能：目录快照已加载后，Catalog 短暂停机不影响已有商品的扫描。
    stop('catalog')
    cached=call(urls['checkout'],'POST','/transactions',{'stationId':'cached-catalog'},201)['transactionId']
    call(urls['checkout'],'POST','/transactions/'+cached+'/items',{'sku':'SKU-000009'})
    start('catalog')
    # 功能：两个购物篮争抢最后一件库存，只能一笔完成，另一笔保持 OPEN。
    sql(databases['inventory'],"UPDATE inventory SET current_stock=1 WHERE sku='SKU-000020'")
    racers=[]
    for _ in range(2):
        ident=call(urls['checkout'],'POST','/transactions',{'stationId':'last-stock-race'},201)['transactionId']
        call(urls['checkout'],'POST','/transactions/'+ident+'/items',{'sku':'SKU-000020'})
        racers.append(ident)
    from concurrent.futures import ThreadPoolExecutor
    def complete_status(ident):
        req=Request(urls['checkout']+'/transactions/'+ident+'/complete',method='POST')
        try:
            with urlopen(req,timeout=15) as response:return response.status
        except HTTPError as error:return error.code
    with ThreadPoolExecutor(max_workers=2) as executor:statuses=list(executor.map(complete_status,racers))
    assert sorted(statuses)==[200,409],statuses
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000020'")=='0'
    # 功能：相反商品排列的重叠购物篮并发预留/释放，不能死锁或丢失库存。
    before_bulk=sql(databases['inventory'],"SELECT json_agg(r ORDER BY sku) FROM (SELECT sku,current_stock FROM inventory) r")
    bulk_ids=['bulk-'+str(uuid.uuid4()) for _ in range(32)]
    def reserve_bulk(pair):
        index,ident=pair
        skus=['SKU-000011','SKU-000012','SKU-000013']
        if index%2:skus.reverse()
        call(urls['inventory'],'POST','/internal/reservations/'+ident,{'items':[{'sku':sku,'quantity':1} for sku in skus]})
    with ThreadPoolExecutor(max_workers=16) as executor:list(executor.map(reserve_bulk,enumerate(bulk_ids)))
    with ThreadPoolExecutor(max_workers=16) as executor:list(executor.map(lambda ident:call(urls['inventory'],'POST','/internal/reservations/'+ident+'/release'),bulk_ids))
    assert sql(databases['inventory'],"SELECT json_agg(r ORDER BY sku) FROM (SELECT sku,current_stock FROM inventory) r")==before_bulk
    print('PASS: cached Catalog outage, last-stock race, concurrent overlapping basket reserve/release',flush=True)
    tx=call(urls['checkout'],'POST','/transactions',{'stationId':'lost-confirmation'},201)['transactionId']
    path='/transactions/'+tx
    call(urls['checkout'],'POST',path+'/items',{'sku':'SKU-000003'})
    Proxy.lose_confirm=True
    call(urls['checkout'],'POST',path+'/complete',expected=503)
    assert call(urls['checkout'],'GET',path)['status']=='CHECKOUT_PENDING'
    assert sql(databases['checkout'],"SELECT state FROM checkout_sagas WHERE transaction_id='"+tx+"'")=='RESERVING'
    attempt=sql(databases['checkout'],"SELECT saga_id FROM checkout_sagas WHERE transaction_id='"+tx+"'")
    assert sql(databases['inventory'],"SELECT status FROM reservations WHERE transaction_id='"+attempt+"'")=='CONFIRMED'
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000003'")=='999'
    # 功能：扣减响应丢失后仍在 RESERVING；新实例必须通过同一 ID 幂等恢复，不再扣库存。
    sql(databases['checkout'],"UPDATE checkout_sagas SET lease_owner='crashed-worker',lease_until=now()-interval '1 second',next_attempt_at=now() WHERE transaction_id='"+tx+"'")
    stop('checkout');start('checkout')
    deadline=time.monotonic()+15
    while call(urls['checkout'],'GET',path)['status']!='COMPLETED':
        assert time.monotonic()<deadline,'Recovery timeout'
        time.sleep(.2)
    receipt=call(urls['checkout'],'POST',path+'/complete')
    assert receipt['itemCount']==1
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000003'")=='999'
    assert sql(databases['checkout'],"SELECT state FROM checkout_sagas WHERE transaction_id='"+tx+"'")=='DONE'
    # 功能：保留上一版 RESERVED / COMMITTING 检查点的兼容恢复覆盖。
    legacy_tx=call(urls['checkout'],'POST','/transactions',{'stationId':'legacy-committing'},201)['transactionId']
    legacy_path='/transactions/'+legacy_tx
    call(urls['checkout'],'POST',legacy_path+'/items',{'sku':'SKU-000016'})
    legacy_attempt=str(uuid.uuid4())
    call(urls['inventory'],'POST','/internal/reservations/'+legacy_attempt,{'items':[{'sku':'SKU-000016','quantity':1}]})
    sql(databases['inventory'],"UPDATE reservations SET status='RESERVED' WHERE transaction_id='"+legacy_attempt+"'")
    sql(databases['checkout'],"BEGIN; INSERT INTO checkout_sagas(saga_id,transaction_id,state) VALUES ('"+legacy_attempt+"','"+legacy_tx+"','COMMITTING'); UPDATE checkout_transactions SET status='CHECKOUT_PENDING',version=version+1 WHERE transaction_id='"+legacy_tx+"'; COMMIT;")
    assert call(urls['checkout'],'POST',legacy_path+'/complete')['itemCount']==1
    assert sql(databases['inventory'],"SELECT status FROM reservations WHERE transaction_id='"+legacy_attempt+"'")=='CONFIRMED'
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000016'")=='999'
    print('PASS: lost debit response recovers once; legacy COMMITTING remains compatible',flush=True)
    # 功能：恢复后没有新请求也可继续 RESERVING；Inventory 停机期间交易保持冻结。
    tx2=call(urls['checkout'],'POST','/transactions',{'stationId':'inventory-outage'},201)['transactionId']
    path2='/transactions/'+tx2
    call(urls['checkout'],'POST',path2+'/items',{'sku':'SKU-000004'})
    stop('inventory')
    call(urls['checkout'],'POST',path2+'/complete',expected=503)
    assert call(urls['checkout'],'GET',path2)['status']=='CHECKOUT_PENDING'
    call(urls['checkout'],'POST',path2+'/items',{'sku':'SKU-000004'},409)
    start('inventory');stop('checkout');start('checkout')
    deadline=time.monotonic()+15
    while call(urls['checkout'],'GET',path2)['status']!='COMPLETED':
        assert time.monotonic()<deadline,'Outage recovery timeout'
        time.sleep(.2)
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000004'")=='999'
    # 功能：远程确认完成后换掉租约；旧工作者必须被 fencing，不能更新交易或清除新租约。
    stale=call(urls['checkout'],'POST','/transactions',{'stationId':'stale-worker'},201)['transactionId']
    stale_path='/transactions/'+stale
    call(urls['checkout'],'POST',stale_path+'/items',{'sku':'SKU-000017'})
    Proxy.confirm_committed.clear();Proxy.confirm_resume.clear();Proxy.pause_confirm=True
    with ThreadPoolExecutor(max_workers=1) as executor:
        future=executor.submit(lambda:call(urls['checkout'],'POST',stale_path+'/complete',expected=503))
        try:
            assert Proxy.confirm_committed.wait(10),'Confirmation did not reach proxy'
            sql(databases['checkout'],"UPDATE checkout_sagas SET lease_owner='new-owner',lease_until=now()+interval '20 seconds' WHERE transaction_id='"+stale+"'")
        finally:Proxy.confirm_resume.set()
        future.result()
    assert call(urls['checkout'],'GET',stale_path)['status']=='CHECKOUT_PENDING'
    assert sql(databases['checkout'],"SELECT lease_owner FROM checkout_sagas WHERE transaction_id='"+stale+"'")=='new-owner'
    sql(databases['checkout'],"UPDATE checkout_sagas SET lease_until=now()-interval '1 second' WHERE transaction_id='"+stale+"'")
    stale_receipt=call(urls['checkout'],'POST',stale_path+'/complete')
    assert stale_receipt['itemCount']==1
    assert sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000017'")=='999'
    print('PASS: stale lease owner fenced, new lease preserved, takeover confirms once',flush=True)
    print('PASS: isolated PostgreSQL, concurrent completion, rollback, lost confirmation, Checkout restart, Inventory outage recovery')
    # 功能：Analytics 停机时扫描和结账仍成功；事件跨 Checkout 重启保留。
    stop('analytics')
    tx3=call(urls['checkout'],'POST','/transactions',{'stationId':'analytics-backlog'},201)['transactionId']
    path3='/transactions/'+tx3
    for n in range(1000):
        call(urls['checkout'],'POST',path3+'/items',{'sku':'SKU-000005' if n%3 else 'SKU-000006'})
    short=call(urls['checkout'],'POST','/transactions',{'stationId':'analytics-offline-checkout'},201)['transactionId']
    call(urls['checkout'],'POST','/transactions/'+short+'/items',{'sku':'SKU-000007'})
    call(urls['checkout'],'POST','/transactions/'+short+'/complete')
    assert int(sql(databases['checkout'],"SELECT count(*) FROM scan_outbox WHERE processed=false"))>=1001
    stop('checkout');start('checkout')
    assert int(sql(databases['checkout'],"SELECT count(*) FROM scan_outbox WHERE processed=false"))>=1001
    Proxy.lose_ack=True
    start('analytics')
    def wait_for(check,description,timeout=45):
        deadline=time.monotonic()+timeout
        while not check():
            assert time.monotonic()<deadline,description
            time.sleep(.2)
    wait_for(lambda:sql(databases['checkout'],"SELECT count(*) FROM scan_outbox WHERE processed=false")=='0','outbox drain')
    total=int(sql(databases['checkout'],"SELECT count(*) FROM scan_outbox"))
    wait_for(lambda:int(sql(databases['analytics'],"SELECT count(*) FROM scan_events"))==total,'event promotion')
    assert Proxy.ack_losses==1,'fault injection not exercised'
    source=json.loads(sql(databases['checkout'],"SELECT json_agg(e ORDER BY event_key) FROM (SELECT event_key,sku,item_name AS name FROM scan_outbox) e"))
    received=json.loads(sql(databases['analytics'],"SELECT json_agg(e ORDER BY event_key) FROM (SELECT event_key,sku,name FROM scan_events) e"))
    assert source==received,'events lost, duplicated, or payload changed'
    assert int(sql(databases['analytics'],"SELECT max(sequence) FROM scan_events"))==total,'sequence gap'
    expected_end=total//500*500
    wait_for(lambda:call(urls['analytics'],'GET','/analytics/popular-items')['windowEnd']==expected_end,'ranking publication')
    ranking=call(urls['analytics'],'GET','/analytics/popular-items')
    first=max(1,expected_end-999)
    expected=json.loads(sql(databases['analytics'],f"SELECT json_agg(r) FROM (SELECT sku,count(*) AS count FROM scan_events WHERE sequence BETWEEN {first} AND {expected_end} GROUP BY sku ORDER BY count(*) DESC,sku LIMIT 10) r"))
    assert ranking['windowStart']==first
    assert [(r['sku'],r['scanCount']) for r in ranking['items']]==[(r['sku'],r['count']) for r in expected]
    for n,item in enumerate(ranking['items'],1):
        assert item['rank']==n and item['name'].startswith('Store Item ')
    # 功能：重复批次确认、冲突批次整体回滚、无效请求均不增加分析事件。
    event=source[0]
    batch={'events':[{'eventId':event['event_key'],'sku':event['sku'],'name':event['name']}]}
    call(urls['analytics'],'POST','/internal/events/scans',batch)
    repeated={'events':batch['events']*2}
    assert call(urls['analytics'],'POST','/internal/events/scans',repeated)['acceptedEventIds']==[event['event_key']]*2
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _:call(urls['analytics'],'POST','/internal/events/scans',repeated),range(16)))
    bad={'events':[{'eventId':str(uuid.uuid4()),'sku':'NEW','name':'should rollback'},
                   {'eventId':event['event_key'],'sku':event['sku'],'name':'changed payload'}]}
    call(urls['analytics'],'POST','/internal/events/scans',bad,409)
    fresh=str(uuid.uuid4())
    conflicting={'events':[{'eventId':fresh,'sku':'NEW','name':'first'},{'eventId':fresh,'sku':'NEW','name':'conflict'}]}
    call(urls['analytics'],'POST','/internal/events/scans',conflicting,409)
    call(urls['analytics'],'POST','/internal/events/scans',{'events':[{'eventId':'invalid'}]},400)
    assert int(sql(databases['analytics'],"SELECT count(*) FROM scan_inbox"))==total
    stop('analytics');start('analytics')
    assert call(urls['analytics'],'GET','/analytics/popular-items')==ranking,'restart changed published ranking'
    assert int(sql(databases['analytics'],"SELECT count(*) FROM analytics_windows"))==total//500
    print('PASS: Analytics outage, durable outbox restart, lost ACK deduplication, exact payloads, gap-free sequences, window ranking, conflicting-batch rollback, Analytics restart')
    if '--core-only' not in sys.argv:
        # 功能：验证实际 run.sh，而非另写一套启动逻辑；使用相同临时库检查数据保留。
        for name in list(processes):stop(name)
        envfile=work/'cluster.env'
        import shlex
        lines=[]
        for name,db in databases.items():
            for key,value in {'URL':'jdbc:postgresql://localhost:5432/'+db,'USER':user,'PASSWORD':password}.items():
                lines.append(name.upper()+'_DB_'+key+'='+shlex.quote(value))
        envfile.write_text('\n'.join(lines)+'\n');envfile.chmod(0o600)
        def free_block():
            for _ in range(100):
                base=port()
                if base>65531:continue
                sockets=[]
                try:
                    for offset in range(5):
                        probe=socket.socket();sockets.append(probe);probe.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);probe.bind(('127.0.0.1',base+offset))
                    return base
                except OSError:pass
                finally:
                    for probe in sockets:probe.close()
            raise RuntimeError('No five-port block available')
        managed_base=free_block()
        managed_url='http://127.0.0.1:'+str(managed_base)
        def start_runner():
            log=open(work/('runner-'+str(len(logs))+'.log'),'w');logs.append(log)
            process=subprocess.Popen([str(root/'run.sh'),'--skip-build','--env-file',str(envfile),'--base-port',str(managed_base),'--log-dir',str(work/'managed')],stdout=log,stderr=subprocess.STDOUT)
            processes['runner']=process
            deadline=time.monotonic()+90
            while time.monotonic()<deadline:
                assert process.poll() is None,'Runner exited; logs: '+str(work)
                try:
                    if call(managed_url,'GET','/health')['service']=='gateway':return
                except (URLError,OSError,AssertionError):pass
                time.sleep(.25)
            raise AssertionError('Runner startup timeout')
        # 功能：被占用端口应在初始化/启动前报告，不终止占用者。
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1',managed_base));occupied.listen()
            rejected=subprocess.run([str(root/'run.sh'),'--skip-build','--env-file',str(envfile),'--base-port',str(managed_base)],capture_output=True,text=True,timeout=15)
            assert rejected.returncode!=0 and 'occupied' in rejected.stderr
        stock_before=int(sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000008'"))
        start_runner()
        assert len(call(managed_url,'GET','/items')['items'])==20
        assert call(managed_url,'GET','/inventory/low-stock?threshold=1001')['threshold']==1001
        call(managed_url,'GET','/analytics/popular-items')
        call(managed_url,'POST','/internal/events/scans',{'events':[]},404)
        call(managed_url,'POST','/internal/reservations/private',{'items':[]},404)
        gateway_tx=call(managed_url,'POST','/transactions',{'stationId':'gateway-smoke'},201)['transactionId']
        gp='/transactions/'+gateway_tx
        call(managed_url,'POST',gp+'/items',{'sku':'SKU-000008'})
        receipt=call(managed_url,'POST',gp+'/complete')
        assert call(managed_url,'POST',gp+'/complete')==receipt
        assert int(sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000008'"))==stock_before-1
        wait_for(lambda:sql(databases['checkout'],"SELECT count(*) FROM scan_outbox WHERE processed=false")=='0','runner outbox delivery')
        total_before=sql(databases['checkout'],"SELECT count(*) FROM checkout_transactions")
        stop('runner')
        # 子进程都应退出，五个端口均可再次绑定。
        for offset in range(5):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);probe.bind(('127.0.0.1',managed_base+offset))
        start_runner()
        assert sql(databases['checkout'],"SELECT count(*) FROM checkout_transactions")==total_before
        assert int(sql(databases['inventory'],"SELECT current_stock FROM inventory WHERE sku='SKU-000008'"))==stock_before-1
        assert call(managed_url,'POST',gp+'/complete')==receipt
        stop('runner')
        print('PASS: run.sh preflight, four DB configs, five processes, public Gateway routes, private endpoints blocked, SIGTERM cleanup, restart preserves stock/transactions/receipt')
    print('Logs:',work)
finally:
    for name in list(processes):stop(name)
    proxy.shutdown();proxy.server_close()
    for log in logs:log.close()
    for db in reversed(created):sql('postgres','DROP DATABASE '+db)
