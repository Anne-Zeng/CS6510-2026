package edu.cs6510.servicebased.inventory;

import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Request;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class InventoryEventsTest {
    private final ValidatorFactory validators = Validation.buildDefaultValidatorFactory();
    private final InventoryService inventory = mock(InventoryService.class);
    private final InventoryEvents endpoint = new InventoryEvents(mock(MessageBus.class), inventory,
            validators.getValidator());

    @AfterEach
    void closeValidator() {
        validators.close();
    }

    @Test
    void invalidKafkaReservationCannotBypassHttpValidation() {
        var reply = endpoint.handle(new Request("request-1", "INVENTORY_RESERVE", "transaction-1",
                "{\"items\":[{\"sku\":\"A\",\"quantity\":0}]}", "replies"));
        assertEquals(400, reply.status());
        verifyNoInteractions(inventory);
    }

    @Test
    void keyRemainsSagaIdForIdempotentInventoryOperations() {
        when(inventory.reserve(eq("transaction-1"), any()))
                .thenReturn(new InventoryService.Reservation("transaction-1", "CONFIRMED"));
        var request = new Request("request-2", "INVENTORY_RESERVE", "transaction-1",
                "{\"items\":[{\"sku\":\"A\",\"quantity\":2}]}", "replies");
        assertEquals(200, endpoint.handle(request).status());
        verify(inventory).reserve(eq("transaction-1"),
                eq(new InventoryService.Request(java.util.List.of(new InventoryService.Line("A", 2)))));
    }

    @Test
    void unknownOperationDoesNotTouchInventory() {
        var reply = endpoint.handle(new Request("request-3", "INVALID", "transaction-1", "{}", "replies"));
        assertEquals(400, reply.status());
        verifyNoInteractions(inventory);
    }
}
