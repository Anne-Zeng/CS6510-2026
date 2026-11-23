package edu.cs6510.notification;

import edu.cs6510.events.Json;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class DeliveryBatchTest {
    @Test void batchingIsOptInAndPreservesEveryDeliveryField() {
        assertFalse(Boolean.TRUE.equals(Json.read("{\"type\":\"SUBSCRIBE\"}", Protocol.Command.class).batchDeliveries()));
        assertTrue(Json.read("{\"type\":\"SUBSCRIBE\",\"batchDeliveries\":true}", Protocol.Command.class).batchDeliveries());
        var completed = Instant.now();
        var delivery = new Protocol.Delivery("SALE", "id", "event", "alice", "transaction", "SKU", 3, completed, null);
        var payload = Json.read(Json.write(SubscriberHandler.deliveryBatch(List.of(delivery))), Map.class);
        assertEquals("SALE_BATCH", payload.get("type"));
        var items = (List<?>) payload.get("deliveries");
        assertEquals(1, items.size());
        var item = (Map<?, ?>) items.getFirst();
        assertEquals("id", item.get("notificationId"));
        assertEquals("alice", item.get("subscriberId"));
        assertEquals("transaction", item.get("transactionId"));
        assertEquals("SKU", item.get("sku"));
        assertEquals(3, item.get("quantity"));
        assertEquals(completed, Instant.parse((String) item.get("completedAt")));
        assertNotNull(item.get("sentAt"));
    }
    @Test void boundsBatchSize() {
        var delivery = new Protocol.Delivery("SALE", "id", "event", "alice", "transaction", "SKU", 1, Instant.now(), null);
        assertThrows(IllegalArgumentException.class, () -> SubscriberHandler.deliveryBatch(List.of()));
        assertThrows(IllegalArgumentException.class, () -> SubscriberHandler.deliveryBatch(java.util.Collections.nCopies(33, delivery)));
    }
}
