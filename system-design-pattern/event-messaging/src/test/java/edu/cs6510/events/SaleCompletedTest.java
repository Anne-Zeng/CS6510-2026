package edu.cs6510.events;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class SaleCompletedTest {
    @Test void roundTripPreservesQuantityAndTimestamp() {
        var event = new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now(), List.of(new SaleCompleted.Item("A", 3)));
        assertEquals(event, Json.read(Json.write(event), SaleCompleted.class));
    }
    @Test void rejectsMalformedEvents() {
        assertThrows(IllegalArgumentException.class, () -> new SaleCompleted.Item("A", 0));
        assertThrows(IllegalArgumentException.class, () -> new SaleCompleted.Item("", 1));
        assertThrows(IllegalArgumentException.class, () -> new SaleCompleted(2, "bad", "bad", Instant.now(), List.of()));
        assertThrows(IllegalArgumentException.class, () -> new SaleCompleted(1, UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), Instant.now(), List.of(new SaleCompleted.Item("A", 1), new SaleCompleted.Item("A", 2))));
    }
}
