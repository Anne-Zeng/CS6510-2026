package edu.cs6510.subscriber;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class DeliveryBatchTest {
    @Test void legacyAndBatchMessagesHaveSameIndividualRecords() {
        var first = Map.of("type", "SALE", "notificationId", "first");
        var second = Map.of("type", "SALE", "notificationId", "second");
        assertEquals(List.of(first), SubscriberMain.salesInMessage(first));
        assertEquals(List.of(first, second), SubscriberMain.salesInMessage(Map.of("type", "SALE_BATCH", "deliveries", List.of(first, second))));
        assertTrue(SubscriberMain.salesInMessage(Map.of("type", "SUBSCRIBED")).isEmpty());
    }
    @Test void malformedBatchFailsInsteadOfSilentlyDroppingRecords() {
        for (Object deliveries : List.of("wrong", List.of(), List.of(Map.of("type", "ERROR")),
                java.util.Collections.nCopies(33, Map.of("type", "SALE")))) {
            assertThrows(IllegalArgumentException.class, () -> SubscriberMain.salesInMessage(Map.of("type", "SALE_BATCH", "deliveries", deliveries)));
        }
    }
}
