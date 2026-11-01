package edu.cs6510.servicebased.catalog;

import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.events.Request;
import edu.cs6510.events.Responses;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

//2注册 Catalog 的 Kafka 请求处理，处理 GET_CATALOG
/** Kafka is the only transport used by Gateway and Checkout to obtain catalog data. */
@Component
public class CatalogEvents {
    private final MessageBus bus;
    private final CatalogService catalog;

    public CatalogEvents(MessageBus bus, CatalogService catalog) {
        this.bus = bus;
        this.catalog = catalog;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        bus.serve("catalog", this::handle);
    }

    Reply handle(Request request) {
        return Responses.call(() -> switch (request.operation()) {
            case "GET_CATALOG" -> catalog.getCatalog();
            default -> throw new ResponseStatusException(BAD_REQUEST, "Unknown catalog operation");
        });
    }
}
