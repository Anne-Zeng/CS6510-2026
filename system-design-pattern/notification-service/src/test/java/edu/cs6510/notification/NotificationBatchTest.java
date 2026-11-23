package edu.cs6510.notification;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class NotificationBatchTest {
    @Test void payloadJoinHappensAfterBoundedPendingSelection() {
        var jdbc = mock(JdbcTemplate.class);
        new NotificationStore(jdbc, 100, 2000).pending("alice");
        var query = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(jdbc).query(query.capture(), any(org.springframework.jdbc.core.RowMapper.class), eq("alice"), eq(512));
        assertTrue(query.getValue().contains("pending AS MATERIALIZED"));
        assertTrue(query.getValue().indexOf("LIMIT ?") < query.getValue().indexOf("JOIN notification_sale_items"));
        assertTrue(query.getValue().contains("ORDER BY next_attempt_at,notification_id LIMIT ?"));
        assertTrue(query.getValue().contains("next_attempt_at<=now()"));
    }
    @Test void metricsOnlyReadBoundedCounterTable() {
        var jdbc = mock(JdbcTemplate.class);
        new NotificationStore(jdbc, 100, 2000).metrics();
        var query = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForMap(query.capture());
        assertTrue(query.getValue().contains("FROM notification_metric_totals"));
        assertFalse(query.getValue().contains("FROM notification_deliveries"));
        assertFalse(query.getValue().contains("count(*)"));
    }
    @Test void combinesOverlappingSendAndAckIntoOneWrite() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(2000L), anyString(), eq("alice"))).thenReturn(List.of("id"));
        store.dispatchBatch("alice", List.of("id", "id"), List.of("id", "id"));
        var payload = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForList(anyString(), eq(String.class), eq(2000L), payload.capture(), eq("alice"));
        var states = edu.cs6510.events.Json.read(payload.getValue(), java.util.List.class);
        assertEquals(List.of(java.util.Map.of("id", "id", "sent", true, "ack", true)), states);
        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }
    @Test void rejectsOversizedStateBeforeDatabaseAccess() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        var identifiers = java.util.stream.IntStream.range(0, 1025).mapToObj(Integer::toString).toList();
        assertThrows(IllegalArgumentException.class, () -> store.dispatchBatch("alice", List.of(), identifiers));
        verifyNoInteractions(jdbc);
    }
    @Test void customPlanningIsEnabledBeforeDispatchSqlOnly() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        org.springframework.test.util.ReflectionTestUtils.setField(store, "dispatchCustomPlan", true);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(2000L), anyString(), eq("alice"))).thenReturn(List.of("id"));
        store.dispatchBatch("alice", List.of(), List.of("id"));
        var order = inOrder(jdbc);
        order.verify(jdbc).execute("SET LOCAL plan_cache_mode=force_custom_plan");
        order.verify(jdbc).queryForList(anyString(), eq(String.class), eq(2000L), anyString(), eq("alice"));
        order.verify(jdbc).query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq("alice"), eq(512));
        verifyNoMoreInteractions(jdbc);
    }
    @Test void emptyDispatchDoesNotReplan() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        org.springframework.test.util.ReflectionTestUtils.setField(store, "dispatchCustomPlan", true);
        store.dispatchBatch("alice", List.of(), List.of());
        verify(jdbc, never()).execute(anyString());
    }
    @Test void rejectsOversizedDispatchBatches() {
        var store = new NotificationStore(mock(JdbcTemplate.class), 100, 2000);
        store.validateDispatchBatchSize();
        for (int size : List.of(0, -1, 513)) {
            org.springframework.test.util.ReflectionTestUtils.setField(store, "dispatchBatchSize", size);
            assertThrows(IllegalArgumentException.class, store::validateDispatchBatchSize);
        }
    }
    @Test void sentBatchUsesOneDatabaseUpdate() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        store.sent(List.of("first", "second"));
        verify(jdbc, times(1)).update(anyString(), eq(2000L), eq("[\"first\",\"second\"]"));
        verifyNoMoreInteractions(jdbc);
    }
    @Test void emptyBatchesDoNotAccessDatabase() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        store.sent(List.of());
        store.acknowledgeBatch("alice", List.of());
        verifyNoInteractions(jdbc);
    }
    @Test void acknowledgementsAreDeduplicatedInOneUpdate() {
        var jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), eq("alice"), eq("[\"first\",\"second\"]"))).thenReturn(2);
        var store = new NotificationStore(jdbc, 100, 2000);
        store.acknowledgeBatch("alice", List.of("second", "first", "first"));
        verify(jdbc, times(1)).update(anyString(), eq("alice"), eq("[\"first\",\"second\"]"));
    }
    @Test void missingOrForeignIdsFailAcknowledgementBatch() {
        var jdbc = mock(JdbcTemplate.class);
        var store = new NotificationStore(jdbc, 100, 2000);
        assertThrows(IllegalArgumentException.class, () -> store.acknowledgeBatch("alice", List.of("foreign")));
    }
}
