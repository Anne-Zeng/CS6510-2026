package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Reply;
import edu.cs6510.events.Responses;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox.PreparedScan;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
//将并发到达的扫描请求放入队列，合并数据库写入，等待提交结果
/**
 * Coalesce scans already waiting for PostgreSQL, without adding a batching delay.
 * A caller receives success only after its basket, outbox and command response
 * have durably committed. The bounded queue never silently drops a command.
 */
@Component
public class ScanWriter implements AutoCloseable {
    private static final int QUEUE_CAPACITY=512;
    private static final long WAIT_MILLIS=6000;
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ScanWriter.class);
    private record Pending(PreparedScan scan,CompletableFuture<Reply> reply) {}

    private final CommandInbox inbox;
    private final boolean enabled;
    private final boolean performanceLog;
    private final long timeoutMillis;
    private final BlockingQueue<Pending> queue;
    private final Object lifecycle=new Object();
    private final Thread worker;
    private volatile boolean running=true;
    private long batches,scans,writeNanos;

    @Autowired
    public ScanWriter(CommandInbox inbox,
            @Value("${checkout.scan.batching-enabled:true}") boolean enabled,
            @Value("${checkout.scan.performance-log:false}") boolean performanceLog) {
        this(inbox,enabled,performanceLog,QUEUE_CAPACITY,WAIT_MILLIS);
    }

    ScanWriter(CommandInbox inbox,boolean enabled,boolean performanceLog,int capacity,long timeoutMillis) {
        if(capacity<1 || timeoutMillis<1) throw new IllegalArgumentException("Invalid scan writer limits");
        this.inbox=inbox;
        this.enabled=enabled;
        this.performanceLog=performanceLog;
        this.timeoutMillis=timeoutMillis;
        this.queue=new ArrayBlockingQueue<>(capacity);
        worker=enabled?Thread.ofPlatform().name("checkout-scan-writer").daemon().start(this::run):null;
    }

    public Reply scan(PreparedScan scan) {
        if(!enabled) {
            synchronized(lifecycle) {
                if(!running) throw unavailable("Checkout stopping; retry the same scan request ID");
            }
            return single(scan);
        }
        //ScanWriter.scan() 入队并等待写入结果
        var pending=new Pending(scan,new CompletableFuture<>());
        synchronized(lifecycle) {
            if(!running) throw unavailable("Checkout stopping; retry the same scan request ID");
            //为这次写入创建任务和结果 Future
            if(!queue.offer(pending)) throw unavailable("Checkout scan queue full; retry the same scan request ID");
        }//将任务放进 ScanWriter 自己的内存队列（这个队列是扫描写入队列，不是Outbox，也不是OutboxPublisher），当前调用线程等待数据库处理结果
        try { return pending.reply().get(timeoutMillis,TimeUnit.MILLISECONDS); }
        catch(TimeoutException failure) {
            // Do not remove an accepted request: it may be committing now. A
            // broker redelivery with the same ID resolves its durable result.
            throw unavailable("Scan outcome pending; retry the same scan request ID");
        } catch(InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw unavailable("Scan interrupted; retry the same scan request ID");
        } catch(ExecutionException failure) {
            if(failure.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException(failure.getCause());
        }
    }
//CommandInbox performs the durable database work. It updates the basket and item lines, records an outbox event, and saves the command result atomically.
    private void run() {
        var waiting=new ArrayList<Pending>();
        try {
            while(running) {
                try {//先取一个任务，再收集已经等待的任务，筛选后调用 write(batch) 批量写入数据库, CommandInbox 执行 SQL，真正写入 Outbox
                    if(waiting.isEmpty()) waiting.add(queue.take());
                    queue.drainTo(waiting,CommandInbox.MAX_SCAN_BATCH_SIZE-waiting.size());
                } catch(InterruptedException stopping) { break; }
                var ids=new HashSet<String>();
                var baskets=new HashSet<String>();
                var batch=new ArrayList<Pending>(waiting.size());
                var later=new ArrayList<Pending>();
                for(var pending:waiting) {
                    var scan=pending.scan();
                    if(ids.contains(scan.requestId()) || baskets.contains(scan.transactionId())) later.add(pending);
                    else {
                        ids.add(scan.requestId());
                        baskets.add(scan.transactionId());
                        batch.add(pending);
                    }
                }
                long started=performanceLog?System.nanoTime():0;
                write(batch);
                if(performanceLog) record(batch.size(),System.nanoTime()-started);
                waiting=later;
            }
        } finally {
            var failure=unavailable("Checkout stopping; retry the same scan request ID");
            waiting.forEach(pending->pending.reply().completeExceptionally(failure));
            var remaining=new ArrayList<Pending>();
            synchronized(lifecycle) { running=false;queue.drainTo(remaining); }
            remaining.forEach(pending->pending.reply().completeExceptionally(failure));
        }
    }

    private void write(List<Pending> batch) {
        if(batch.size()==1) {
            var pending=batch.getFirst();
            pending.reply().complete(single(pending.scan()));
            return;
        }
        try {
            var replies=inbox.scanBatch(batch.stream().map(Pending::scan).toList());
            if(replies.size()!=batch.size() || batch.stream().anyMatch(pending->!replies.containsKey(pending.scan().requestId())))
                throw new IllegalStateException("Incomplete committed scan responses");
            batch.forEach(pending->pending.reply().complete(replies.get(pending.scan().requestId())));
        } catch(RuntimeException batchFailure) {
            // A SQL/constraint failure rolls back the whole statement. Retry each
            // command with the original ID: bad baskets cannot prevent valid
            // baskets from succeeding, and racing replicas cannot double scan.
            batch.forEach(pending->pending.reply().complete(single(pending.scan())));
        }
    }

    private Reply single(PreparedScan scan) {
        try { return inbox.scan(scan.requestId(),scan.transactionId(),scan.item()); }
        catch(RuntimeException failure) { return Responses.call(()->{throw failure;}); }
    }

    private void record(int count,long nanos) {
        batches++;scans+=count;writeNanos+=nanos;
        if(batches%200==0) log.info("Checkout scan batches={} meanScans={} meanWriteMs={}",
                batches,(double)scans/batches,writeNanos/1e6/batches);
    }

    private static ResponseStatusException unavailable(String reason) {
        return new ResponseStatusException(SERVICE_UNAVAILABLE,reason);
    }

    @PreDestroy
    @Override public void close() {
        synchronized(lifecycle) {
            if(!running) return;
            running=false;
            if(worker!=null) worker.interrupt();
        }
        if(worker!=null) {
            try { worker.join(WAIT_MILLIS); }
            catch(InterruptedException failure) { Thread.currentThread().interrupt(); }
        }
    }
}
