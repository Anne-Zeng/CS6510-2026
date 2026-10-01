package edu.cs6510.pipeline_server.analytics;

import edu.cs6510.pipeline_server.persistence.ScanEventStore;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

class AnalyticsPipelineTest {
    @Test void windowsHaveExactBoundariesAndDetectMissingEvents() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(1600L);
        when(events.between(501, 1500)).thenReturn(Collections.nCopies(1000, "A"));
        var builder = new WindowBuilderFilter(events);
        var batch = builder.build(1500).orElseThrow();
        assertEquals(501, batch.firstSequence());
        assertEquals(1500, batch.lastSequence());
        assertEquals(1000, batch.skus().size());
        assertTrue(builder.build(2000).isEmpty());
        when(events.between(1, 500)).thenReturn(List.of("A"));
        assertThrows(IllegalStateException.class, () -> builder.build(500));
    }

    @Test void rankingCountsDuplicatesBreaksTiesAndLimitsToTen() {
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

    @Test void blockedWriterBackpressuresPipesThenCatchesUpInOrder() throws Exception {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(5000L);
        AtomicInteger reads = new AtomicInteger();
        when(events.between(anyLong(), anyLong())).thenAnswer(call -> {
            reads.incrementAndGet();
            long first = call.getArgument(0), last = call.getArgument(1);
            return Collections.nCopies((int)(last-first+1), "A");
        });
        var store = mock(AnalyticsWindowStore.class);
        var releaseWriter = new CountDownLatch(1);
        var writerReached = new CountDownLatch(1);
        var completed = new CountDownLatch(10);
        var saved = new CopyOnWriteArrayList<Long>();
        var attempts = new AtomicInteger();
        doAnswer(call -> {
            writerReached.countDown();
            if (attempts.getAndIncrement() == 0) {
                if (!releaseWriter.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test timed out");
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
            // Two one-slot queues + in-flight messages bound upstream work even
            // though ten windows are ready in the durable source.
            Thread.sleep(100);
            assertTrue(reads.get() <= 5, "bounded pipes must stop the builder from reading all windows");
            releaseWriter.countDown();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(500L,1000L,1500L,2000L,2500L,3000L,3500L,4000L,4500L,5000L), saved);
            assertEquals(11, attempts.get());
        } finally { releaseWriter.countDown(); pipeline.stop(); }
    }

    @Test void restartBeginsAfterLastDurablySavedWindow() throws Exception {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(1600L);
        when(events.between(501,1500)).thenReturn(Collections.nCopies(1000,"B"));
        var store = mock(AnalyticsWindowStore.class);
        when(store.lastPublishedSequence()).thenReturn(1000L);
        var saved = new CountDownLatch(1);
        doAnswer(call -> {
            RankedWindow result = call.getArgument(0);
            assertEquals(1500, result.lastSequence());
            saved.countDown(); return null;
        }).when(store).save(any(),eq(1000),eq(500),eq(10));
        var pipeline = new AnalyticsPipeline(new WindowBuilderFilter(events),new RankerFilter(),
                new ResultWriterFilter(store),store,1,5,5);
        try {
            pipeline.start();
            assertTrue(saved.await(5,TimeUnit.SECONDS));
        } finally { pipeline.stop(); }
        verify(events,never()).between(1,500);
        verify(events,never()).between(1,1000);
        verify(store,times(1)).save(any(),eq(1000),eq(500),eq(10));
    }
}
