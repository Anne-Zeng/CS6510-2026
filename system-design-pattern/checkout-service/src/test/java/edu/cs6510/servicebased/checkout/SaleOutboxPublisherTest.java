package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.MessageBus;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.sql.Timestamp;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SaleOutboxPublisherTest {
    @Test void brokerAcknowledgementPrecedesPersistingTimingAndProcessedState() {
        var jdbc = mock(JdbcTemplate.class);
        var bus = mock(MessageBus.class);
        when(jdbc.query(anyString(), org.mockito.ArgumentMatchers.<RowMapper<String[]>>any()))
                .thenReturn(List.<String[]>of(new String[]{"transaction", "{}"}));
        when(bus.topic("sales")).thenReturn("test.sales");
        assertEquals(1, new SaleOutboxPublisher(jdbc, bus).publishOnce());
        var order = inOrder(bus, jdbc);
        order.verify(bus).publish("test.sales", "transaction", "{}");
        order.verify(jdbc).update(contains("publish_started_at=COALESCE"),
                any(Timestamp.class), any(Timestamp.class), eq("transaction"));
    }
    @Test void failedPublicationDoesNotMarkProcessedOrRecordSuccessTiming() {
        var jdbc = mock(JdbcTemplate.class);
        var bus = mock(MessageBus.class);
        when(jdbc.query(anyString(), org.mockito.ArgumentMatchers.<RowMapper<String[]>>any()))
                .thenReturn(List.<String[]>of(new String[]{"transaction", "{}"}));
        when(bus.topic("sales")).thenReturn("test.sales");
        doThrow(new IllegalStateException("broker unavailable")).when(bus).publish(anyString(), anyString(), anyString());
        assertThrows(IllegalStateException.class, () -> new SaleOutboxPublisher(jdbc, bus).publishOnce());
        verify(jdbc, never()).update(anyString(), any(Timestamp.class), any(Timestamp.class), anyString());
    }
}
