package edu.cs6510.notification;

import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Json;
import edu.cs6510.events.SaleCompleted;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class SalesEventsTest {
    @Test void diagnosticsPreserveTransactionalConsumerCall() {
        var store = mock(NotificationStore.class);
        var event = new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now(), List.of(new SaleCompleted.Item("A", 1)));
        var records = List.of(Json.write(event));
        new SalesEvents(mock(MessageBus.class), store, true, 16).accept(records);
        verify(store).accept(records);
    }
    @Test void failureStillPropagatesForKafkaReplay() {
        var store = mock(NotificationStore.class);
        var records = List.of("{}");
        doThrow(new IllegalStateException("rollback")).when(store).accept(records);
        assertThrows(IllegalStateException.class, () -> new SalesEvents(mock(MessageBus.class), store, true, 16).accept(records));
    }
    @Test void commitsBoundedChunksInOrderIncludingRemainder() {
        var store = mock(NotificationStore.class);
        new SalesEvents(mock(MessageBus.class), store, false, 2).accept(List.of("a", "b", "c", "d", "e"));
        var order = inOrder(store);
        order.verify(store).accept(List.of("a", "b"));
        order.verify(store).accept(List.of("c", "d"));
        order.verify(store).accept(List.of("e"));
        order.verifyNoMoreInteractions();
    }
    @Test void failedChunkStopsProcessingAndReplayRevisitsEarlierChunks() {
        var store = mock(NotificationStore.class);
        var consumer = new SalesEvents(mock(MessageBus.class), store, false, 2);
        var records = List.of("a", "b", "c", "d", "e");
        doThrow(new IllegalStateException("rollback")).doNothing().when(store).accept(List.of("c", "d"));
        assertThrows(IllegalStateException.class, () -> consumer.accept(records));
        verify(store, never()).accept(List.of("e"));
        consumer.accept(records);
        verify(store, times(2)).accept(List.of("a", "b"));
        verify(store, times(2)).accept(List.of("c", "d"));
        verify(store).accept(List.of("e"));
    }
    @Test void emptyCallbackDoesNotOpenTransaction() {
        var store = mock(NotificationStore.class);
        new SalesEvents(mock(MessageBus.class), store, true, 16).accept(List.of());
        verifyNoInteractions(store);
    }
    @Test void rejectsInvalidBatchSizes() {
        for (int size : List.of(0, -1, 257))
            assertThrows(IllegalArgumentException.class,
                    () -> new SalesEvents(mock(MessageBus.class), mock(NotificationStore.class), false, size));
    }
}
