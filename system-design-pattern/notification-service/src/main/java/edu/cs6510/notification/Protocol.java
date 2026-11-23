package edu.cs6510.notification;

import java.time.Instant;
import java.util.List;

public final class Protocol {
    private Protocol() {}
    public record Command(String type, String subscriberId, String token, List<String> skus, String notificationId,
                          List<String> notificationIds, Boolean batchDeliveries) {}
    public record Delivery(String type, String notificationId, String eventId, String subscriberId,
                           String transactionId, String sku, int quantity, Instant completedAt, Instant sentAt) {
        public Delivery forDispatch() {
            return new Delivery(type, notificationId, eventId, subscriberId, transactionId, sku, quantity, completedAt, Instant.now());
        }
    }
}
