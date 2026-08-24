import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a single physical checkout station: repeatedly starts a
 * transaction, scans a random number of items (drawn from the weighted
 * catalog sampler), completes the transaction, and records latency for
 * each of the three operation types. Runs until {@code stopRequested}
 * becomes true, then finishes whatever transaction is in flight before
 * exiting - it never abandons a transaction partway through.
 */
public final class StationWorker implements Runnable {

    private final String stationId;
    private final ApiClient client;
    private final ItemSampler sampler;
    private final Metrics metrics;
    private final int minItems;
    private final int maxItems;
    private final boolean verbose;
    private final AtomicBoolean stopRequested;

    private final AtomicLong transactionsCompleted = new AtomicLong();
    private final AtomicLong itemsScanned = new AtomicLong();

    public StationWorker(String stationId, ApiClient client, ItemSampler sampler, Metrics metrics,
                          int minItems, int maxItems, boolean verbose, AtomicBoolean stopRequested) {
        this.stationId = stationId;
        this.client = client;
        this.sampler = sampler;
        this.metrics = metrics;
        this.minItems = minItems;
        this.maxItems = maxItems;
        this.verbose = verbose;
        this.stopRequested = stopRequested;
    }

    @Override
    public void run() {
        while (!stopRequested.get()) {
            try {
                runOneTransaction();
            } catch (Exception e) {
                // A single failed transaction should never kill the worker -
                // record it and move on to the next customer.
                System.err.println("[" + stationId + "] transaction failed: " + e.getMessage());
            }
        }
    }

    private void runOneTransaction() {
        long t0 = System.nanoTime();
        ApiClient.StartedTransaction started;
        try {
            started = client.startTransaction(stationId);
            metrics.recordSuccess(Metrics.Operation.START_TRANSACTION, System.nanoTime() - t0);
        } catch (ApiClient.ApiException e) {
            metrics.recordError(Metrics.Operation.START_TRANSACTION);
            throw e;
        }

        int basketSize = ThreadLocalRandom.current().nextInt(minItems, maxItems + 1);
        for (int i = 0; i < basketSize; i++) {
            CatalogItem item = sampler.next();
            long tScan = System.nanoTime();
            try {
                client.scanItem(started.transactionId(), item.sku());
                metrics.recordSuccess(Metrics.Operation.SCAN_ITEM, System.nanoTime() - tScan);
                itemsScanned.incrementAndGet();
            } catch (ApiClient.ApiException e) {
                metrics.recordError(Metrics.Operation.SCAN_ITEM);
                throw e;
            }
        }

        long tComplete = System.nanoTime();
        try {
            ApiClient.ReceiptSummary receipt = client.completeTransaction(started.transactionId());
            metrics.recordSuccess(Metrics.Operation.COMPLETE_TRANSACTION, System.nanoTime() - tComplete);
            transactionsCompleted.incrementAndGet();
            if (verbose) {
                System.out.printf("[%s] tx=%s items=%d total=%.2f%n",
                        stationId, started.transactionId(), receipt.itemCount(), receipt.totalAmount());
            }
        } catch (ApiClient.ApiException e) {
            metrics.recordError(Metrics.Operation.COMPLETE_TRANSACTION);
            throw e;
        }
    }

    public long transactionsCompleted() {
        return transactionsCompleted.get();
    }

    public long itemsScanned() {
        return itemsScanned.get();
    }
}
