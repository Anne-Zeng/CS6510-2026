package edu.cs6510.layered_server.analytics;

import edu.cs6510.layered_server.persistence.ScanEventStore;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PersistentScanWindowTest {
    @Test void exactWindowsCatchUpAndRetryWithoutAdvancing() {
        var events = mock(ScanEventStore.class);
        var log = new ArrayList<String>();
        for (int i = 0; i < 1600; i++) log.add(i < 500 ? "A" : "B");
        when(events.count()).thenReturn(1600L);
        when(events.between(anyLong(), anyLong())).thenAnswer(call ->
                log.subList((int)((long)call.getArgument(0)) - 1, (int)((long)call.getArgument(1))));
        var window = new PersistentScanWindow(events);
        var first = window.slideIfDue().orElseThrow();
        assertEquals(500, first.lastSequence());
        assertEquals(List.of(new ScanWindow.SkuCount("A", 500)), first.ranked());
        assertEquals(first, window.slideIfDue().orElseThrow());
        window.resumeAfter(500);
        var second = window.slideIfDue().orElseThrow();
        assertEquals(1000, second.lastSequence());
        assertEquals(List.of(new ScanWindow.SkuCount("A", 500), new ScanWindow.SkuCount("B", 500)), second.ranked());
        window.resumeAfter(1000);
        var third = window.slideIfDue().orElseThrow();
        assertEquals(501, third.firstSequence());
        assertEquals(1500, third.lastSequence());
        assertEquals(List.of(new ScanWindow.SkuCount("B", 1000)), third.ranked());
        window.resumeAfter(1500);
        assertTrue(window.slideIfDue().isEmpty());
    }

    @Test void missingEventsFailInsteadOfPublishingAnIncorrectRanking() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(500L);
        when(events.between(1, 500)).thenReturn(List.of("A"));
        assertThrows(IllegalStateException.class, () -> new PersistentScanWindow(events).slideIfDue());
    }

    @Test void publicationFailureIsRetriedBeforeCheckpointAdvances() {
        var events = mock(ScanEventStore.class);
        when(events.count()).thenReturn(500L);
        when(events.between(1, 500)).thenReturn(Collections.nCopies(500, "A"));
        var window = new PersistentScanWindow(events);
        var store = mock(AnalyticsWindowStore.class);
        doThrow(new IllegalStateException("injected storage failure")).doNothing()
                .when(store).save(any(), eq(1000), eq(500), eq(10));
        var service = new AnalyticsService(window, store);
        service.resumeWindow();
        service.publishWindowIfDue();
        assertEquals(500, window.slideIfDue().orElseThrow().lastSequence());
        service.publishWindowIfDue();
        assertTrue(window.slideIfDue().isEmpty());
        verify(store, times(2)).save(any(), eq(1000), eq(500), eq(10));
    }
}
