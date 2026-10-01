package edu.cs6510.pipeline_server.analytics;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import edu.cs6510.pipeline_server.analytics.PipelineData.RankedWindow;
import edu.cs6510.pipeline_server.analytics.PipelineData.WindowBatch;

import java.util.concurrent.*; //BlockingQueue 是 Java 自带的线程安全队列接口
import java.util.concurrent.atomic.AtomicBoolean;
import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

//流水线管理者：创建队列和线程，连接三个阶段，处理等待、重试和停止Creates queues, runs three worker threads, handles retries and shutdown
// Each filter runs on its own thread. The queues connect them asynchronously. Full queues make upstream stages wait rather than discard data.
// The API still saves the basket update and scan event in one transaction. After a restart, the pipeline rebuilds unfinished windows from the durable scan log.
/** Three single-consumer stages connected by bounded, FIFO, in-memory pipes.
 * Only database results are checkpoints. Enqueued windows are not acknowledged
 * as persisted; a restart reconstructs all pending work from the durable log.
 */
@Component
public class AnalyticsPipeline {
    private static final Logger log = LoggerFactory.getLogger(AnalyticsPipeline.class);
    private final WindowBuilderFilter builder;
    private final RankerFilter ranker;
    private final ResultWriterFilter writer;
    private final AnalyticsWindowStore store;
    private final BlockingQueue<WindowBatch> windows; //交接口，规定队列提供哪些操作
    private final BlockingQueue<RankedWindow> rankings;
    private final long pollMillis;
    private final long retryMillis;
    private final AtomicBoolean running = new AtomicBoolean();
    private ExecutorService workers;

    public AnalyticsPipeline(WindowBuilderFilter builder, RankerFilter ranker,
            ResultWriterFilter writer, AnalyticsWindowStore store,
//默认容量为 32，意思是：
// - 第一个队列最多等待 32 个窗口；
// - 第二个队列最多等待 32 份排名。
            @Value("${analytics.pipeline.queue-capacity:32}") int capacity,
            @Value("${analytics.pipeline.poll-ms:25}") long pollMillis,
            @Value("${analytics.pipeline.retry-ms:250}") long retryMillis) {
        if (capacity < 1 || pollMillis < 1 || retryMillis < 1)
            throw new IllegalArgumentException("Pipeline capacity and delays must be positive");
        this.builder = builder;
        this.ranker = ranker;
        this.writer = writer;
        this.store = store;
        this.windows = new ArrayBlockingQueue<>(capacity);//具体实现，使用固定容量保存数据
        this.rankings = new ArrayBlockingQueue<>(capacity);
        this.pollMillis = pollMillis;
        this.retryMillis = retryMillis;
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (running.get()) return;
        //保存失败时，Writer 会保留当前的 pending 排名并重试，成功后才处理下一个窗口。
        //两个队列都在内存里，重启后里面的数据会消失。因此，启动时会执行：
        long saved = store.lastPublishedSequence();
        if (saved % SLIDE_INTERVAL != 0) throw new IllegalStateException("Invalid saved window boundary");
        windows.clear();
        rankings.clear();
        running.set(true);
        workers = Executors.newFixedThreadPool(3);//创建三个线程，分别执行三个阶段的任务
        workers.execute(() -> buildWindows(saved + SLIDE_INTERVAL));//WindowBuilderFilter.build()
        workers.execute(this::rankWindows);//RankerFilter.rank()
        workers.execute(this::writeResults);//ResultWriterFilter.write()
    }

    private void buildWindows(long nextEnd) {
        Thread.currentThread().setName("analytics-window-builder");
        while (running.get()) {
            try {
                var next = builder.build(nextEnd);
                if (next.isEmpty()) { Thread.sleep(pollMillis); continue; }
                windows.put(next.get()); // A full pipe blocks; never discard a window. put(data)放入数据；队列满了就等待有空位
                nextEnd += SLIDE_INTERVAL; // Enqueue position, NOT a durable checkpoint.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return;
            } catch (RuntimeException e) {
                if (!retry("window builder", e)) return;
            }
        }
    }

    private void rankWindows() {
        Thread.currentThread().setName("analytics-ranker");
        WindowBatch pending = null;
        while (running.get()) {
            try {
                if (pending == null) pending = windows.take();//取出最早加入的数据；队列空了就等待新数据
                rankings.put(ranker.rank(pending));//put(data)放入数据；队列满了就等待有空位
                pending = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return;
            } catch (RuntimeException e) {
                if (!retry("ranker", e)) return; // Keep the same input for retry.
            }
        }
    }

    private void writeResults() {
        Thread.currentThread().setName("analytics-result-writer");
        RankedWindow pending = null;
        while (running.get()) {
            try {
                if (pending == null) pending = rankings.take();//取出最早加入的数据；队列空了就等待新数据
                writer.write(pending); // Returns only after the store transaction commits.
                pending = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return;
            } catch (RuntimeException e) {
                if (!retry("result writer", e)) return; // Do not consume a later window.
            }
        }
    }

    private boolean retry(String stage, RuntimeException failure) {
        log.warn("Analytics {} failed; retaining the current window for retry", stage, failure);
        try { Thread.sleep(retryMillis); return running.get(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
    }

    @PreDestroy
    public synchronized void stop() {
        running.set(false);
        if (workers == null) return;
        workers.shutdownNow(); // Wake blocked put/take/sleep operations.
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS))
                log.warn("Pipeline workers are still stopping; unsaved windows replay on restart");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
