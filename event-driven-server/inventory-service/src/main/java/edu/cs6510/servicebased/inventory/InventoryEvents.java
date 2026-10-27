package edu.cs6510.servicebased.inventory;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.events.Request;
import edu.cs6510.events.Responses;
import jakarta.validation.Validator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

/** Dispatches Kafka inventory commands to the same idempotent local business operations. */
@Component
public class InventoryEvents {
    public record LowStockQuery(Integer threshold) {}

    private final MessageBus bus;
    private final InventoryService inventory;
    private final Validator validator;

    public InventoryEvents(MessageBus bus, InventoryService inventory, Validator validator) {
        this.bus = bus;
        this.inventory = inventory;
        this.validator = validator;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        bus.serve("inventory", this::handle);
    }

    Reply handle(Request request) {
        return Responses.call(() -> switch (request.operation()) {
            case "GET_LOW_STOCK" -> inventory.lowStock(Json.read(request.body(), LowStockQuery.class).threshold());
            case "INVENTORY_RESERVE" -> {
                var reservation = Json.read(request.body(), InventoryService.Request.class);
                Responses.validate(validator, reservation);
                yield inventory.reserve(request.key(), reservation);
            }
            case "INVENTORY_GET" -> inventory.get(request.key());
            case "INVENTORY_CONFIRM" -> inventory.confirm(request.key());
            case "INVENTORY_RELEASE" -> inventory.release(request.key());
            default -> throw new ResponseStatusException(BAD_REQUEST, "Unknown inventory operation");
        });
    }
}
