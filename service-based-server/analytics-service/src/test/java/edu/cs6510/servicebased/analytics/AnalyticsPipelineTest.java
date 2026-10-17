package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.persistence.ScanEventStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.cs6510.servicebased.analytics.PipelineData.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 验证从共享数据库日志读取后的窗口边界、排序、背压、重试和恢复行为。 */
class AnalyticsPipelineTest {
    /** 原始事件整理后达到边界，才允许读取并发布完整窗口。 */
    @Test
    void committedOutboxIsPromotedBeforePublishingNewWindow() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(499L, 500L);
        when(events.promotePending()).thenReturn(1);
        when(events.between(1, 500)).thenReturn(Collections.nCopies(500, "A"));

        var batch = new WindowBuilderFilter(events).build(500).orElseThrow();
        assertEquals(1, batch.firstSequence());
        assertEquals(500, batch.lastSequence());
        assertEquals(500, batch.skus().size());
        verify(events).promotePending();
    }

    /** 最后一批少于 500 条时仍需整理落库，但不能发布未满边界的窗口。 */
    @Test
    void trailingOutboxBatchIsPromotedWithoutPublishingPartialWindow() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(0L, 250L);
        when(events.promotePending()).thenReturn(250);

        assertTrue(new WindowBuilderFilter(events).build(500).isEmpty());
        verify(events).promotePending();
        verify(events, never()).between(anyLong(), anyLong());
    }

    /** 数据不足时不发布；日志缺口必须报错，不能悄悄生成错误排名。 */
    @Test
    void windowsHaveExactBoundariesAndDetectMissingEvents() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(1600L);
        when(events.between(501, 1500)).thenReturn(Collections.nCopies(1000, "A"));
        var builder = new WindowBuilderFilter(events);
        var batch = builder.build(1500).orElseThrow();
        assertEquals(501, batch.firstSequence());
        assertEquals(1500, batch.lastSequence());
        assertEquals(1000, batch.skus().size());
        assertTrue(builder.build(2000).isEmpty());

        // 故意模拟缺失事件，确保算法识别不完整的持久日志。
        when(events.between(1, 500)).thenReturn(List.of("A"));
        assertThrows(IllegalStateException.class, () -> builder.build(500));
        assertThrows(IllegalArgumentException.class, () -> builder.build(501));
    }

    /** 相同次数按 SKU 确定顺序；输出只取前十名并且禁止修改。 */
    @Test
    void rankingCountsDuplicatesBreaksTiesAndLimitsToTen() {
        var skus = new ArrayList<String>();
        for (int i = 11; i >= 0; i--) skus.add(String.format("SKU-%02d", i));
        skus.add("SKU-11");
        var ranked = new RankerFilter().rank(new WindowBatch(1, 13, skus));
        assertEquals(10, ranked.ranked().size());
        assertEquals(new SkuCount("SKU-11", 2), ranked.ranked().get(0));
        assertEquals(new SkuCount("SKU-00", 1), ranked.ranked().get(1));
        assertEquals(new SkuCount("SKU-08", 1), ranked.ranked().get(9));
        assertThrows(UnsupportedOperationException.class, () -> ranked.ranked().clear());
    }

    /** 阻塞写入会阻止上游无限读取；故障恢复后所有窗口仍应按顺序写入。 */
    @Test
    void blockedWriterBackpressuresPipesThenCatchesUpInOrder() throws Exception {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(5000L);
        AtomicInteger reads = new AtomicInteger();
        when(events.between(anyLong(), anyLong())).thenAnswer(call -> {
            reads.incrementAndGet();
            long first = call.getArgument(0), last = call.getArgument(1);
            return Collections.nCopies((int) (last - first + 1), "A");
        });

        // 第一次写入先阻塞再失败，验证队列上限和 pending 重试，而不是跳过失败窗口。
        var store = mock(AnalyticsWindowStore.class);
        var releaseWriter = new CountDownLatch(1);
        var writerReached = new CountDownLatch(1);
        var completed = new CountDownLatch(10);
        var saved = new CopyOnWriteArrayList<Long>();
        var attempts = new AtomicInteger();
        doAnswer(call -> {
            writerReached.countDown();
            if (attempts.getAndIncrement() == 0) {
                if (!releaseWriter.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test timed out");
                }
                throw new IllegalStateException("injected write failure");
            }
            RankedWindow window = call.getArgument(0);
            saved.add(window.lastSequence());
            completed.countDown();
            return null;
        }).when(store).save(any(), eq(1000), eq(500), eq(10));
        var pipeline = new AnalyticsPipeline(new WindowBuilderFilter(events), new RankerFilter(),
                new ResultWriterFilter(store), store, 1, 5, 5);
        try {
            pipeline.start();
            assertTrue(writerReached.await(5, TimeUnit.SECONDS));
            // 两个单槽队列和各阶段当前持有的窗口，最多允许 Builder 读取五个窗口。
            Thread.sleep(100);
            assertTrue(reads.get() <= 5, "bounded pipes must stop the builder from reading all windows");
            releaseWriter.countDown();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(500L, 1000L, 1500L, 2000L, 2500L,
                    3000L, 3500L, 4000L, 4500L, 5000L), saved);
            assertEquals(11, attempts.get());
        } finally {
            releaseWriter.countDown();
            pipeline.stop();
        }
    }

    /** 服务重启只能采用已持久化窗口的边界，不重复处理更早的窗口。 */
    @Test
    void restartBeginsAfterLastDurablySavedWindow() throws Exception {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(1600L);
        when(events.between(501, 1500)).thenReturn(Collections.nCopies(1000, "B"));
        var store = mock(AnalyticsWindowStore.class);
        when(store.lastPublishedSequence()).thenReturn(1000L);
        var saved = new CountDownLatch(1);
        doAnswer(call -> {
            RankedWindow result = call.getArgument(0);
            assertEquals(1500, result.lastSequence());
            saved.countDown();
            return null;
        }).when(store).save(any(), eq(1000), eq(500), eq(10));
        var pipeline = new AnalyticsPipeline(new WindowBuilderFilter(events), new RankerFilter(),
                new ResultWriterFilter(store), store, 1, 5, 5);
        try {
            pipeline.start();
            assertTrue(saved.await(5, TimeUnit.SECONDS));
        } finally {
            pipeline.stop();
        }
        verify(events, never()).between(1, 500);
        verify(events, never()).between(1, 1000);
        verify(store, times(1)).save(any(), eq(1000), eq(500), eq(10));
    }
}
