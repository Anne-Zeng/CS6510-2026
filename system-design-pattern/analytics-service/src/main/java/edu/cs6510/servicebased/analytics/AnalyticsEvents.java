package edu.cs6510.servicebased.analytics;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.events.Request;
import edu.cs6510.events.Responses;
import edu.cs6510.servicebased.analytics.common.BusinessException;
import edu.cs6510.servicebased.analytics.persistence.ScanEventStore;
import jakarta.validation.Validator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static edu.cs6510.servicebased.analytics.PipelineData.SLIDE_INTERVAL;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
//订阅扫描事件，并处理热门商品 Kafka 查询
/** Kafka scan batches commit to the durable inbox before the bus commits their offsets. */
@Component
public class AnalyticsEvents {
    public record PopularItemsQuery(Integer limit, long minimumScans) {}

    private final MessageBus bus;
    private final AnalyticsService analytics;
    private final EventInbox inbox;
    private final ScanEventStore scans;
    private final AnalyticsWindowStore windows;
    private final Validator validator;
    private final long catchUpMillis;

    public AnalyticsEvents(MessageBus bus, AnalyticsService analytics, EventInbox inbox,
            ScanEventStore scans, AnalyticsWindowStore windows, Validator validator,
            @Value("${analytics.query-catch-up-ms:6000}") long catchUpMillis) {
        if (catchUpMillis < 1) throw new IllegalArgumentException("Query catch-up timeout must be positive");
        this.bus = bus;
        this.analytics = analytics;
        this.inbox = inbox;
        this.scans = scans;
        this.windows = windows;
        this.validator = validator;
        this.catchUpMillis = catchUpMillis;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        bus.subscribe(bus.topic("scans"), "analytics-scans", this::accept);
        bus.serve("analytics", this::handle);
    }

    Reply handle(Request request) {
        return Responses.call(() -> {
            if (!"GET_POPULAR_ITEMS".equals(request.operation())) {
                throw new ResponseStatusException(BAD_REQUEST, "Unknown analytics operation");
            }
            var query = Json.read(request.body(), PopularItemsQuery.class);
            Integer limit = query.limit();
            if (limit != null && limit < 1) {
                throw new BusinessException(BusinessException.Kind.INVALID_REQUEST,
                        "INVALID_REQUEST", "limit must be greater than zero");
            }
            if (query.minimumScans() < 0) {
                throw new ResponseStatusException(BAD_REQUEST, "minimumScans must not be negative");
            }
            awaitPublishedScans(query.minimumScans());
            return analytics.getPopularItems(limit);
        });
    }

    /** Combines small Kafka records into bounded local transactions, retaining inbox ID deduplication. */
    void accept(List<String> records) {
        var pending = new ArrayList<EventInbox.Event>(1000);
        for (String record : records) {
            var batch = Json.read(record, EventInbox.Batch.class);
            Responses.validate(validator, batch);
            for (var event : batch.events()) {
                pending.add(event);
                if (pending.size() == 1000) {
                    inbox.accept(new EventInbox.Batch(List.copyOf(pending)));
                    pending.clear();
                }
            }
        }
        if (!pending.isEmpty()) inbox.accept(new EventInbox.Batch(List.copyOf(pending)));
    }

    /** Gateway supplies Checkout's durable scan checkpoint; separate consumers keep ingestion moving. */
    private void awaitPublishedScans(long minimumScans) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(catchUpMillis);
        while (scans.count() < minimumScans) {
            pauseOrFail(deadline);
        }
        long target = Math.max(scans.count(), minimumScans) / SLIDE_INTERVAL * SLIDE_INTERVAL;
        while (target > windows.lastPublishedSequence()) {
            pauseOrFail(deadline);
        }
    }

    private void pauseOrFail(long deadline) {
        if (System.nanoTime() >= deadline) {
            throw new ResponseStatusException(SERVICE_UNAVAILABLE, "Analytics is catching up; retry query");
        }
        try {
            Thread.sleep(10);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(SERVICE_UNAVAILABLE, "Analytics query interrupted", failure);
        }
    }
}
