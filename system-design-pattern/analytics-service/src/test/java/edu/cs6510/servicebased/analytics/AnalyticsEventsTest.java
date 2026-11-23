package edu.cs6510.servicebased.analytics;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Request;
import edu.cs6510.servicebased.analytics.contract.PopularItemsResponse;
import edu.cs6510.servicebased.analytics.persistence.ScanEventStore;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises offset callback boundaries and read consistency without a running Kafka cluster. */
class AnalyticsEventsTest {
    private final ValidatorFactory validators = Validation.buildDefaultValidatorFactory();
    private final MessageBus bus = mock(MessageBus.class);
    private final AnalyticsService analytics = mock(AnalyticsService.class);
    private final EventInbox inbox = mock(EventInbox.class);
    private final ScanEventStore scans = mock(ScanEventStore.class);
    private final AnalyticsWindowStore windows = mock(AnalyticsWindowStore.class);

    @AfterEach
    void closeValidator() {
        validators.close();
    }

    private AnalyticsEvents endpoint(long timeoutMillis) {
        return new AnalyticsEvents(bus, analytics, inbox, scans, windows,
                validators.getValidator(), timeoutMillis);
    }

    private List<EventInbox.Event> events(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> new EventInbox.Event(UUID.randomUUID().toString(), "SKU-A", "Apple"))
                .toList();
    }

    @Test
    void polledRecordsCommitAsBoundedInboxTransactionsInOriginalOrder() {
        var events = events(1250);
        endpoint(100).accept(List.of(
                Json.write(new EventInbox.Batch(events.subList(0, 500))),
                Json.write(new EventInbox.Batch(events.subList(500, 1000))),
                Json.write(new EventInbox.Batch(events.subList(1000, 1250)))));

        var batches = ArgumentCaptor.forClass(EventInbox.Batch.class);
        verify(inbox, times(2)).accept(batches.capture());
        assertEquals(events.subList(0, 1000), batches.getAllValues().get(0).events());
        assertEquals(events.subList(1000, 1250), batches.getAllValues().get(1).events());
    }

    @Test
    void invalidEventPreventsDatabaseCommitAndMakesPollCallbackFail() {
        String record = Json.write(new EventInbox.Batch(List.of(new EventInbox.Event("invalid-id", "A", "Apple"))));
        assertThrows(RuntimeException.class, () -> endpoint(100).accept(List.of(record)));
        verifyNoInteractions(inbox);
    }

    @Test
    void databaseFailurePropagatesSoKafkaOffsetsAreNotAcknowledged() {
        doThrow(new IllegalStateException("database unavailable")).when(inbox).accept(any());
        String record = Json.write(new EventInbox.Batch(events(1)));
        assertThrows(IllegalStateException.class, () -> endpoint(100).accept(List.of(record)));
    }

    @Test
    void popularQueryWaitsForCheckpointAndItsCompleteRankingWindow() {
        when(scans.count()).thenReturn(499L, 500L, 500L);
        when(windows.lastPublishedSequence()).thenReturn(0L, 500L);
        var result = new PopularItemsResponse(1000, 500, 1, 500, Instant.now(), List.of());
        when(analytics.getPopularItems(3)).thenReturn(result);

        var reply = endpoint(1000).handle(new Request("query-1", "GET_POPULAR_ITEMS", "",
                "{\"limit\":3,\"minimumScans\":500}", "replies"));

        assertEquals(200, reply.status());
        assertEquals(result, Json.read(reply.body(), PopularItemsResponse.class));
        var order = inOrder(scans, windows, analytics);
        order.verify(scans, atLeast(1)).count();
        order.verify(windows, times(2)).lastPublishedSequence();
        order.verify(analytics).getPopularItems(3);
    }

    @Test
    void unavailableCheckpointReturnsRetryableFailureInsteadOfStaleSuccess() {
        when(scans.count()).thenReturn(0L);
        var reply = endpoint(1).handle(new Request("query-2", "GET_POPULAR_ITEMS", "",
                "{\"minimumScans\":500}", "replies"));
        assertEquals(503, reply.status());
        verifyNoInteractions(analytics, windows);
    }

    @Test
    void invalidLimitIsRejectedBeforeWaitingForCheckpoint() {
        var reply = endpoint(100).handle(new Request("query-3", "GET_POPULAR_ITEMS", "",
                "{\"limit\":0,\"minimumScans\":500}", "replies"));
        assertEquals(400, reply.status());
        verifyNoInteractions(scans, windows, analytics);
    }
}
