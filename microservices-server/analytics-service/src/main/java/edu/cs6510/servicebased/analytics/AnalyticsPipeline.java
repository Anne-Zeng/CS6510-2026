package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.PipelineData.RankedWindow;
import edu.cs6510.servicebased.analytics.PipelineData.WindowBatch;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static edu.cs6510.servicebased.analytics.PipelineData.SLIDE_INTERVAL;

/** 管理本服务内三段分析线程；接收 HTTP 事件后只处理本地持久日志。 */
@Component
public class AnalyticsPipeline {
    // 每段使用单个消费者保持窗口顺序；两个固定容量队列让慢写入自动阻塞上游。
    private static final Logger log = LoggerFactory.getLogger(AnalyticsPipeline.class);
    private final WindowBuilderFilter builder;
    private final RankerFilter ranker;
    private final ResultWriterFilter writer;
    private final AnalyticsWindowStore store;
    private final BlockingQueue<WindowBatch> windows;
    private final BlockingQueue<RankedWindow> rankings;

    // 轮询间隔决定无新事件时的等待时间；失败重试保留当前窗口而不跳过数据。
    private final long pollMillis;
    private final long retryMillis;
    private final AtomicBoolean running = new AtomicBoolean();
    private ExecutorService workers;

    /** 创建队列并校验参数；线程等到数据库和 Web 应用准备完成后才启动。 */
    public AnalyticsPipeline(WindowBuilderFilter builder, RankerFilter ranker,
            ResultWriterFilter writer, AnalyticsWindowStore store,
            @Value("${analytics.pipeline.queue-capacity:32}") int capacity,
            @Value("${analytics.pipeline.poll-ms:25}") long pollMillis,
            @Value("${analytics.pipeline.retry-ms:250}") long retryMillis) {
        if (capacity < 1 || pollMillis < 1 || retryMillis < 1) {
            throw new IllegalArgumentException("Pipeline capacity and delays must be positive");
        }
        this.builder = builder;
        this.ranker = ranker;
        this.writer = writer;
        this.store = store;
        this.windows = new ArrayBlockingQueue<>(capacity);
        this.rankings = new ArrayBlockingQueue<>(capacity);
        this.pollMillis = pollMillis;
        this.retryMillis = retryMillis;
    }

    /** 从最后已提交窗口恢复；内存队列丢失不会导致已提交扫描记录丢失。 */
    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (running.get()) return;
        long saved = store.lastPublishedSequence();
        if (saved % SLIDE_INTERVAL != 0) {
            throw new IllegalStateException("Invalid saved window boundary");
        }
        windows.clear();
        rankings.clear();
        running.set(true);
        workers = Executors.newFixedThreadPool(3);
        workers.execute(() -> buildWindows(saved + SLIDE_INTERVAL));
        workers.execute(this::rankWindows);
        workers.execute(this::writeResults);
    }

    /** 按顺序读取已提交日志；没有完整窗口时等待，队列满时阻塞而不丢弃窗口。 */
    private void buildWindows(long nextEnd) {
        Thread.currentThread().setName("analytics-window-builder");
        while (running.get()) {
            try {
                var next = builder.build(nextEnd);
                if (next.isEmpty()) {
                    Thread.sleep(pollMillis);
                    continue;
                }
                windows.put(next.get());
                // 此位置只表示已入队；只有数据库提交的窗口才能成为恢复检查点。
                nextEnd += SLIDE_INTERVAL;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (!retry("window builder", e)) return;
            }
        }
    }

    /** 计算下一窗口排名；异常时保留 pending，让重试处理相同输入。 */
    private void rankWindows() {
        Thread.currentThread().setName("analytics-ranker");
        WindowBatch pending = null;
        while (running.get()) {
            try {
                if (pending == null) pending = windows.take();
                rankings.put(ranker.rank(pending));
                pending = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (!retry("ranker", e)) return;
            }
        }
    }

    /** 严格按窗口顺序提交排名；一次提交成功后才清空 pending 并读取下一个。 */
    private void writeResults() {
        Thread.currentThread().setName("analytics-result-writer");
        RankedWindow pending = null;
        while (running.get()) {
            try {
                if (pending == null) pending = rankings.take();
                writer.write(pending);
                pending = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (!retry("result writer", e)) return;
            }
        }
    }

    /** 记录故障并短暂退避，防止数据库短暂不可用时忙循环。 */
    private boolean retry(String stage, RuntimeException failure) {
        log.warn("Analytics {} failed; retaining the current window for retry", stage, failure);
        try {
            Thread.sleep(retryMillis);
            return running.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 关闭进程时唤醒所有阻塞线程；未提交窗口会在下次启动时重新计算。 */
    @PreDestroy
    public synchronized void stop() {
        running.set(false);
        if (workers == null) return;
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Pipeline workers are still stopping; unsaved windows replay on restart");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
