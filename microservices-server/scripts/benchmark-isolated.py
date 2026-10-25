#!/usr/bin/env python3
"""用原 load-client 测量 default/stress；临时数据库独立初始化，保留原始报告与审计。
测试账号需能创建数据库，并能通过本地 socket/localhost 登录。
"""
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import argparse
import getpass
import hashlib
import json
import os
import platform
import signal
import shlex
import shutil
import socket
import subprocess
import sys
import time
import uuid
import tempfile
from system import Cluster, Database, ROOT, SERVICES, fetch_json


def require(condition, message):
    if not condition: raise RuntimeError(message)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--modes',nargs='+',choices=('default','stress'),default=['default','stress'])
    parser.add_argument('--database-servers',choices=('shared','separate'),default='shared',help='Four logical databases on the existing server, or four temporary PostgreSQL instances with 32MB shared_buffers each')
    parser.add_argument('--commit-delay-micros',type=int,default=0,help='Optional PostgreSQL durable group-commit delay; requires an admin test account')
    parser.add_argument('--java-opts',default='-Xms64m -Xmx256m',help='Service JVM options; default uses normal tiered compilation (C2 enabled)')
    parser.add_argument('--checkout-inventory-max-in-flight',type=int,default=8)
    parser.add_argument('--inventory-max-in-flight',type=int,default=8)
    parser.add_argument('--virtual-threads',action='store_true',help='Enable Spring virtual request threads; disabled unless explicitly requested')
    parser.add_argument('--publish-submission',action='store_true',help='Back up and update reports/submission only after both workloads pass all audits')
    args=parser.parse_args()
    require(0<=args.commit_delay_micros<=100000,'Invalid commit delay')
    require(args.checkout_inventory_max_in_flight>0 and args.inventory_max_in_flight>0,'Concurrency limits must be positive')
    require(not args.publish_submission or set(args.modes)=={'default','stress'},'Publishing requires both default and stress workloads')
    java_options=shlex.split(args.java_opts)
    user=os.environ.get('SAGA_TEST_DB_USER',getpass.getuser())
    password=os.environ.get('SAGA_TEST_DB_PASSWORD','')
    opening=1000000
    catalog_size=2000
    output=ROOT/'reports'/datetime.now(ZoneInfo('America/Los_Angeles')).strftime('load-%Y%m%d-%H%M%S')
    output.mkdir(parents=True)
    def admin(statement,port=5432):
        result=subprocess.run(['psql','-X','-h','127.0.0.1','-p',str(port),'-U',user,'-d','postgres','-Atq','-v','ON_ERROR_STOP=1','-c',statement],
                              env={**os.environ,'PGPASSWORD':password},capture_output=True,text=True)
        require(result.returncode==0,result.stderr)
        return result.stdout.strip()
    classes=output/'load-client-classes';classes.mkdir()
    sources=sorted((ROOT.parent/'load-client/src').glob('*.java'))
    subprocess.run(['javac','--release','21','-d',str(classes),*map(str,sources)],check=True)
    def free_block():
        for _ in range(100):
            with socket.socket() as probe:probe.bind(('127.0.0.1',0));base=probe.getsockname()[1]
            if base>65531:continue
            probes=[]
            try:
                for offset in range(5):
                    probe=socket.socket();probes.append(probe);probe.bind(('127.0.0.1',base+offset))
                return base
            except OSError:pass
            finally:
                for probe in probes:probe.close()
        raise RuntimeError('No free five-port block')
    metadata={'startedAt':datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(),
              'commitDelayMicros':args.commit_delay_micros,'commitSiblings':0 if args.commit_delay_micros else 5,
              'catalogSize':catalog_size,'initialStockPerSku':opening,'databaseServers':args.database_servers,'sharedBuffersPerTemporaryServer':'32MB' if args.database_servers=='separate' else None,
              'jvmOptions':java_options,
              'springVirtualThreads':args.virtual_threads,
              'poolLimits':{'catalog':8,'checkout':16,'analytics':8,'inventory':8},
              'checkoutInventoryMaxInFlight':args.checkout_inventory_max_in_flight,'wholeSagaConcurrencyCap':None,'inventoryMaxInFlight':args.inventory_max_in_flight,
              'scanMode':'parallel single-statement autocommit; no application scan queue',
              'catalogReadMode':'immutable local SKU map loaded once via Catalog API',
              'outboxMaxBatchesPerPoll':10,'canonicalEventWrite':'atomic with inbox receipt','outboxBatchSize':500,'outboxAcknowledgementSqlPerBatch':1,'rankingWriteSqlPerWindow':1,
              'platform':platform.platform(),'logicalCpuCount':os.cpu_count(),
              'loadAverageAtStart':list(os.getloadavg()),
              'javaVersion':subprocess.run(['java','-version'],capture_output=True,text=True).stderr.strip(),
              'postgresqlVersion':admin('SELECT version()'),
              'synchronousCommit':admin('SHOW synchronous_commit'),
              'loadClientSourceSha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
              'serviceSchemaSha256':{name:hashlib.sha256((ROOT/(name+'-service')/'database/schema.sql').read_bytes()).hexdigest() for name in SERVICES},
              'serviceJarSha256':{name:hashlib.sha256((ROOT/(name+'-service')/'target'/(name+'-service-0.0.1-SNAPSHOT.jar')).read_bytes()).hexdigest() for name in SERVICES},
              'gatewayJarSha256':hashlib.sha256((ROOT/'gateway/target/gateway-0.0.1-SNAPSHOT.jar').read_bytes()).hexdigest(),
              'comparison':'Historical service-based-server/reports/submission reports; different run time and background load, one run per workload'}
    (output/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
    print('Reports: '+str(output),flush=True)
    results={}
    for mode in args.modes:
        folder=output/mode;folder.mkdir()
        suffix=uuid.uuid4().hex[:12]
        names={name:'microbench_'+name+'_'+suffix for name in SERVICES}
        created=[];cluster=None
        temporary_servers=[];temporary_directory=None
        database_ports={name:5432 for name in SERVICES}
        env={**os.environ,'MICROSERVICES_JAVA_OPTS':args.java_opts,
             'CHECKOUT_INVENTORY_MAX_IN_FLIGHT':str(args.checkout_inventory_max_in_flight),
             'INVENTORY_MAX_IN_FLIGHT':str(args.inventory_max_in_flight),
             'SPRING_THREADS_VIRTUAL_ENABLED':str(args.virtual_threads).lower(),
             'CHECKOUT_OUTBOX_POLL_MS':'250','INVENTORY_PERFORMANCE_LOG':'true','CHECKOUT_INVENTORY_PERFORMANCE_LOG':'true'}
        try:
            if args.database_servers=='separate':
                temporary_directory=tempfile.TemporaryDirectory(prefix='microbench_pg_',dir='/tmp')
                for name in SERVICES:
                    base=Path(temporary_directory.name)/name;base.mkdir()
                    data=base/'data'
                    subprocess.run(['initdb','-D',str(data),'-U',user,'-A','trust','--locale=C','-E','UTF8'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,check=True)
                    database_ports[name]=free_block()
                    options=f'-p {database_ports[name]} -h 127.0.0.1 -k {base} -c shared_buffers=32MB -c max_connections=32 -c synchronous_commit=on -c fsync=on -c full_page_writes=on'
                    subprocess.run(['pg_ctl','-D',str(data),'-l',str(folder/(name+'-postgresql.log')),'-o',options,'-w','start'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,check=True)
                    temporary_servers.append(data)
            for name in SERVICES:
                env.update({name.upper()+'_DB_URL':'jdbc:postgresql://localhost:'+str(database_ports[name])+'/'+names[name],
                            name.upper()+'_DB_USER':user,name.upper()+'_DB_PASSWORD':password,
                            name.upper()+'_DB_POOL':str(16 if name=='checkout' else 8)})
            db={name:Database(name,env) for name in SERVICES}
            deployment={}
            for name in SERVICES:
                deployment[name]={'port':database_ports[name],
                    'clusterStartedAt':admin('SELECT pg_postmaster_start_time()',database_ports[name]),
                    'sharedBuffers':admin('SHOW shared_buffers',database_ports[name]),
                    'synchronousCommit':admin('SHOW synchronous_commit',database_ports[name]),
                    'fsync':admin('SHOW fsync',database_ports[name]),
                    'walSyncMethod':admin('SHOW wal_sync_method',database_ports[name])}
            (folder/'database-deployment.json').write_text(json.dumps(deployment,indent=2)+'\n')
            for name in SERVICES:
                admin('CREATE DATABASE '+names[name],database_ports[name]);created.append((names[name],database_ports[name]))
                if args.commit_delay_micros:
                    admin('ALTER DATABASE '+names[name]+' SET commit_delay='+str(args.commit_delay_micros),database_ports[name])
                    admin('ALTER DATABASE '+names[name]+' SET commit_siblings=0',database_ports[name])
                db[name].initialize(seed=False)
                deployment[name]['commitDelayMicros']=db[name].sql('SHOW commit_delay')
                deployment[name]['commitSiblings']=db[name].sql('SHOW commit_siblings')
                deployment[name]['databaseCollation']=db[name].sql("SELECT datcollate FROM pg_database WHERE datname=current_database()")
                deployment[name]['databaseCtype']=db[name].sql("SELECT datctype FROM pg_database WHERE datname=current_database()")
            (folder/'database-deployment.json').write_text(json.dumps(deployment,indent=2)+'\n')
            db['catalog'].sql(f"INSERT INTO catalog_items(sku,name,price) SELECT 'SKU-'||lpad(n::text,6,'0'),'Store Item '||lpad(n::text,6,'0'),(100+n%1000)::numeric/100 FROM generate_series(1,{catalog_size}) n")
            db['inventory'].sql(f"INSERT INTO inventory(sku,name,current_stock) SELECT 'SKU-'||lpad(n::text,6,'0'),'Store Item '||lpad(n::text,6,'0'),{opening} FROM generate_series(1,{catalog_size}) n")
            cluster=Cluster(db,folder/'logs',base_port=free_block(),env=env)
            cluster.start()
            command=['java','-cp',str(classes),'Main','--baseUrl='+cluster.base_url,'--reportDir='+str(folder)]
            if mode=='stress':command+=['--stations=100','--duration=120']
            print(f'Running {mode}: '+('10 stations / 60 seconds' if mode=='default' else '100 stations / 120 seconds'),flush=True)
            with open(folder/'client.log','w') as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
            reports=list(folder.glob('report-*.json'));require(len(reports)==1,'Expected one original client report')
            report=json.loads(reports[0].read_text())
            (output/('report-'+mode+'.json')).write_text(reports[0].read_text())
            errors=sum(op['errorCount'] for op in report['operations'])
            print(f"{mode}: {report['transactionsPerSecond']:.2f} transactions/sec; {errors} request errors; draining Analytics",flush=True)
            raw=int(db['checkout'].sql('SELECT count(*) FROM scan_outbox'))
            target=raw//500*500
            drain_started=time.monotonic()
            deadline=drain_started+180
            drained=False
            while time.monotonic()<deadline:
                pending=int(db['checkout'].sql('SELECT count(*) FROM scan_outbox WHERE processed=false'))
                promoted=int(db['analytics'].sql('SELECT count(*) FROM scan_events'))
                if pending==0 and promoted==raw and fetch_json(cluster.base_url+'/analytics/popular-items')['windowEnd']==target:
                    drained=True;break
                time.sleep(.5)
            catchup_seconds=time.monotonic()-drain_started
            (folder/'popular-items-after-drain.json').write_text(json.dumps(fetch_json(cluster.base_url+'/analytics/popular-items'),indent=2)+'\n')
            cluster.stop()
            checks={}
            def check(name,condition):checks[name]=bool(condition)
            def scalar(service,statement):return int(db[service].sql(statement))
            check('client_requests_have_no_errors',errors==0)
            warnings=(folder/'client.log').read_text()
            check('no_client_final_query_or_shutdown_warnings',not any(marker in warnings for marker in ('Could not fetch','not all stations finished cleanly','Failed to write JSON')))
            check('analytics_drained',drained)
            check('completed_transactions_match_client',scalar('checkout',"SELECT count(*) FROM checkout_transactions WHERE status='COMPLETED'")==report['totalTransactions'])
            check('all_transactions_completed',scalar('checkout',"SELECT count(*) FROM checkout_transactions WHERE status<>'COMPLETED'")==0)
            check('all_sagas_done',scalar('checkout',"SELECT count(*) FROM checkout_sagas WHERE state<>'DONE'")==0)
            check('scan_events_match_client',raw==report['totalItemsScanned'])
            check('basket_totals_correct',scalar('checkout',"SELECT count(*) FROM checkout_transactions t LEFT JOIN (SELECT transaction_id,sum(quantity) AS qty,sum(quantity*unit_price) AS total FROM transaction_items GROUP BY transaction_id) l USING(transaction_id) WHERE t.item_count<>COALESCE(l.qty,0) OR t.running_total<>COALESCE(l.total,0)")==0)
            check('outbox_matches_basket_quantity',raw==scalar('checkout','SELECT COALESCE(sum(quantity),0) FROM transaction_items'))
            check('inventory_nonnegative_and_decrement_exact',scalar('inventory',f"SELECT count(*) FROM inventory i LEFT JOIN (SELECT sku,sum(quantity) AS quantity FROM reservation_items l JOIN reservations r USING(transaction_id) WHERE r.status='CONFIRMED' GROUP BY sku) usage USING(sku) WHERE i.current_stock<0 OR i.current_stock<>{opening}-COALESCE(usage.quantity,0)")==0)
            expected=db['checkout'].sql("SELECT COALESCE(json_agg(r ORDER BY saga_id,sku),'[]'::json) FROM (SELECT s.saga_id,l.sku,l.quantity FROM checkout_sagas s JOIN transaction_items l USING(transaction_id) WHERE s.state='DONE') r")
            actual=db['inventory'].sql("SELECT COALESCE(json_agg(r ORDER BY saga_id,sku),'[]'::json) FROM (SELECT r.transaction_id AS saga_id,l.sku,l.quantity FROM reservations r JOIN reservation_items l USING(transaction_id) WHERE r.status='CONFIRMED') r")
            check('confirmed_inventory_matches_completed_baskets',json.loads(expected)==json.loads(actual))
            check('no_unfinished_inventory_reservations',scalar('inventory',"SELECT count(*) FROM reservations WHERE status NOT IN ('CONFIRMED','CANCELLED')")==0)
            source=db['checkout'].sql("SELECT md5(COALESCE(string_agg(event_key||'|'||sku||'|'||item_name,E'\\n' ORDER BY event_key),'')) FROM scan_outbox")
            dest=db['analytics'].sql("SELECT md5(COALESCE(string_agg(event_key||'|'||sku||'|'||name,E'\\n' ORDER BY event_key),'')) FROM scan_events")
            check('event_id_and_payload_checksum_match',source==dest)
            check('canonical_sequence_contiguous',scalar('analytics','SELECT count(*) FROM scan_events')==raw and scalar('analytics','SELECT COALESCE(max(sequence),0) FROM scan_events')==raw)
            check('all_due_windows_published',scalar('analytics','SELECT count(*) FROM analytics_windows')==raw//500 and scalar('analytics','SELECT COALESCE(max(window_end),0) FROM analytics_windows')==target)
            check('window_boundaries_correct',scalar('analytics','SELECT count(*) FROM analytics_windows WHERE window_size<>1000 OR slide_interval<>500 OR window_end%500<>0 OR window_start<>GREATEST(1,window_end-999)')==0)
            mismatches=scalar('analytics',"""WITH counts AS (
                SELECT w.id AS window_id,e.sku,count(*) AS scans FROM analytics_windows w
                JOIN scan_events e ON e.sequence BETWEEN w.window_start AND w.window_end GROUP BY w.id,e.sku
              ), ordered AS (
                SELECT *,row_number() OVER(PARTITION BY window_id ORDER BY scans DESC,sku) AS rank FROM counts
              ), expected AS (SELECT * FROM ordered WHERE rank<=10)
              SELECT count(*) FROM expected e FULL OUTER JOIN popular_item_results r ON e.window_id=r.window_id AND e.sku=r.sku
              WHERE e.window_id IS NULL OR r.window_id IS NULL OR e.scans<>r.scan_count OR e.rank<>r.rank""")
            check('every_window_ranking_independently_recomputed',mismatches==0)
            audit={'passed':all(checks.values()),'checks':checks,'totalScans':raw,'dueWindows':target//500,'clientRequestErrors':errors,'analyticsCatchUpSeconds':catchup_seconds,
                   'databaseCompletedTransactions':scalar('checkout',"SELECT count(*) FROM checkout_transactions WHERE status='COMPLETED'"),
                   'clientAcknowledgedTransactions':report['totalTransactions']}
            (folder/'correctness.json').write_text(json.dumps(audit,indent=2)+'\n')
            results[mode]={'report':report,'auditPassed':audit['passed']}
            print(mode+' audit: '+('PASS' if audit['passed'] else 'FAIL '+str([k for k,v in checks.items() if not v])),flush=True)
        finally:
            try:
                if cluster:cluster.stop()
                for name,port in reversed(created):admin('DROP DATABASE '+name,port)
            finally:
                for data in reversed(temporary_servers):subprocess.run(['pg_ctl','-D',str(data),'-m','fast','-w','stop'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True)
                if temporary_directory:temporary_directory.cleanup()
    summary={}
    for mode,result in results.items():
        previous_path=ROOT.parent/'service-based-server/reports/submission'/('report-'+mode+'.json')
        previous=json.loads(previous_path.read_text());current=result['report']
        completion=lambda report:next(op for op in report['operations'] if op['operation']=='COMPLETE_TRANSACTION')
        summary[mode]={'historicalServiceBasedReport':str(previous_path),'historicalGeneratedAt':previous['generatedAt'],
                       'serviceBasedTps':previous['transactionsPerSecond'],'microservicesTps':current['transactionsPerSecond'],
                       'throughputChangePercent':(current['transactionsPerSecond']/previous['transactionsPerSecond']-1)*100,
                       'serviceBasedCompleteP95Ms':completion(previous)['p95Ms'],'microservicesCompleteP95Ms':completion(current)['p95Ms'],
                       'serviceBasedCompleteP99Ms':completion(previous)['p99Ms'],'microservicesCompleteP99Ms':completion(current)['p99Ms'],
                       'microservicesErrors':sum(op['errorCount'] for op in current['operations']),
                       'auditPassed':result['auditPassed']}
        user_reference=ROOT/'reports/comparison/service-based-user-reference-20261016.json'
        if user_reference.is_file():
            reference=json.loads(user_reference.read_text())[mode]
            summary[mode].update(userProvidedServiceBasedReference=str(user_reference),
                                 userProvidedServiceBasedTps=reference['transactionsPerSecond'],
                                 changeVsUserProvidedPercent=(current['transactionsPerSecond']/reference['transactionsPerSecond']-1)*100)
    (output/'comparison.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)
    print('Reports: '+str(output),flush=True)
    require(all(r['auditPassed'] for r in results.values()),'At least one workload failed correctness checks; see correctness.json')
    if args.publish_submission:
        submission=ROOT/'reports/submission'
        if submission.exists():shutil.copytree(submission,output/'previous-submission')
        submission.mkdir(parents=True,exist_ok=True)
        for mode in ('default','stress'):
            shutil.copy2(output/('report-'+mode+'.json'),submission/('report-'+mode+'.json'))
            shutil.copy2(output/mode/'correctness.json',submission/('correctness-'+mode+'.json'))
            shutil.copy2(output/mode/'database-deployment.json',submission/('database-deployment-'+mode+'.json'))
        for name in ('environment.json','comparison.json'):shutil.copy2(output/name,submission/name)
        (submission/'README.md').write_text(
            '# Current benchmark reports\n\nSource run: `'+str(output.relative_to(ROOT))+'`.\n\n'
            'Both required workloads passed all correctness audits before these files were published. '
            'Current evidence: report-default/stress.json, correctness-default/stress.json, '
            'database-deployment-default/stress.json, environment.json, comparison.json. '
            'Other validation files are historical and do not certify this run. '
            'The previous submission was backed up inside the source run directory.\n')
        print('Submission reports: '+str(submission),flush=True)


if __name__=='__main__':
    signal.signal(signal.SIGTERM,lambda *_:sys.exit(143))
    main()
