package edu.cs6510.notification;

import edu.cs6510.events.Json;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class AcknowledgementBatchTest {
    @Test void acceptsLegacyAndBoundedBatchCommands() {
        String id = UUID.randomUUID().toString();
        var single = Json.read("{\"type\":\"ACK\",\"notificationId\":\"" + id + "\"}", Protocol.Command.class);
        var batch = Json.read("{\"type\":\"ACKS\",\"notificationIds\":[\"" + id + "\"]}", Protocol.Command.class);
        assertEquals(List.of(id), SubscriberHandler.acknowledgementIds(single));
        assertEquals(List.of(id), SubscriberHandler.acknowledgementIds(batch));
    }
    @Test void rejectsMalformedAndOversizedBatches() {
        for (var ids : List.of(List.<String>of(), List.of("bad"), List.of("z".repeat(36)), java.util.Collections.nCopies(65, UUID.randomUUID().toString()))) {
            var command = new Protocol.Command("ACKS", null, null, null, null, ids, false);
            assertThrows(IllegalArgumentException.class, () -> SubscriberHandler.acknowledgementIds(command));
        }
        assertThrows(IllegalArgumentException.class, () -> SubscriberHandler.acknowledgementIds(
                new Protocol.Command("ACKS", null, null, null, null, null, false)));
        assertThrows(IllegalArgumentException.class, () -> SubscriberHandler.acknowledgementIds(
                new Protocol.Command("ACK", null, null, null, null, null, false)));
    }
}
