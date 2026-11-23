package edu.cs6510.notification;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DispatchDatabase {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DispatchDatabase.class);
    @Value("${notification.stage-timing:false}")
    private boolean stageTiming;
    @Value("${notification.ack-drain-at-gate:false}")
    private boolean ackDrainAtGate;
    private final NotificationStore store;
    private final Semaphore permits;

    public DispatchDatabase(NotificationStore store,
            @Value("${notification.dispatch-db-concurrency:1}") int concurrency,
            @Value("${spring.datasource.hikari.maximum-pool-size:8}") int poolSize) {
        if (concurrency < 1 || concurrency > poolSize - 2)
            throw new IllegalArgumentException("Dispatch concurrency must leave at least two pool connections available");
        this.store = store;
        permits = new Semaphore(concurrency, true);
    }

    <T> T execute(Supplier<T> operation) throws InterruptedException {
        permits.acquire();
        try {
            return operation.get();
        } finally {
            permits.release();
        }
    }

    public List<Protocol.Delivery> pending(String subscriberId) throws InterruptedException {
        return execute(() -> store.pending(subscriberId));
    }

    public List<Protocol.Delivery> nextBatch(String subscriberId, List<String> sentIds, List<String> acknowledgedIds)
            throws InterruptedException {
        return nextBatch(subscriberId, sentIds, () -> acknowledgedIds);
    }

    public List<Protocol.Delivery> nextBatch(String subscriberId, List<String> sentIds,
            Supplier<List<String>> acknowledgementSupplier) throws InterruptedException {
        var earlyAcknowledgements = ackDrainAtGate ? null : acknowledgementSupplier.get();
        long started = System.nanoTime();
        return execute(() -> {
            long entered = System.nanoTime();
            var acknowledgedIds = ackDrainAtGate ? acknowledgementSupplier.get() : earlyAcknowledgements;
            var deliveries = store.dispatchBatch(subscriberId, sentIds, acknowledgedIds);
            if (stageTiming)
                log.info("NOTIFICATION_DISPATCH_CALL {}", edu.cs6510.events.Json.write(java.util.Map.of(
                        "gateMs", (entered-started)/1_000_000.0,
                        "transactionMs", (System.nanoTime()-entered)/1_000_000.0,
                        "fetched", deliveries.size(), "writes", sentIds.size()+acknowledgedIds.size())));
            return deliveries;
        });
    }

    public void acknowledgeBatch(String subscriberId, List<String> identifiers) throws InterruptedException {
        if (identifiers.isEmpty()) return;
        execute(() -> { store.acknowledgeBatch(subscriberId, identifiers); return null; });
    }

    public void sent(List<String> identifiers) throws InterruptedException {
        if (identifiers.isEmpty()) return;
        execute(() -> { store.sent(identifiers); return null; });
    }

    public int waitingThreads() { return permits.getQueueLength(); }
}
