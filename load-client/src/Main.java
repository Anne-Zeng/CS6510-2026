import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Self-Checkout Load Client.
 *
 * Simulates N concurrent checkout stations (customers), each repeatedly
 * starting a transaction, scanning a random-sized basket of items, and
 * completing the transaction - against whatever server currently implements
 * the shared self-checkout API contract. This class, and everything it
 * calls, is never modified between architecture weeks: only the server
 * changes.
 *
 * Uses one virtual thread per simulated station (Java 21+), so raising
 * --stations into the hundreds for a stress-test run doesn't require any
 * async/reactive rewrite - it's still "one thread per station" under the
 * hood, just backed by the JVM's lightweight virtual threads instead of OS
 * threads.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);

        System.out.println("Self-Checkout Load Client starting...");
        System.out.printf("  baseUrl=%s stations=%d duration=%ds basket=[%d..%d]%n",
                config.baseUrl, config.stations, config.durationSeconds, config.minItems, config.maxItems);

        ApiClient client = new ApiClient(config.baseUrl, Duration.ofSeconds(config.requestTimeoutSeconds));

        System.out.println("Fetching catalog...");
        List<CatalogItem> catalog = client.fetchCatalog();
        if (catalog.isEmpty()) {
            System.err.println("Catalog is empty - is the server running and seeded? Aborting.");
            System.exit(1);
        }
        System.out.println("Catalog loaded: " + catalog.size() + " items.");

        ItemSampler sampler = new ItemSampler(catalog);
        Metrics metrics = new Metrics();
        AtomicBoolean stopRequested = new AtomicBoolean(false);

        List<StationWorker> workers = new ArrayList<>(config.stations);
        for (int i = 0; i < config.stations; i++) {
            String stationId = "station-" + String.format("%03d", i + 1);
            workers.add(new StationWorker(stationId, client, sampler, metrics,
                    config.minItems, config.maxItems, config.verbose, stopRequested));
        }

        System.out.println("Starting " + config.stations + " simulated station(s) for " + config.durationSeconds + "s...");
        long startNanos = System.nanoTime();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (StationWorker worker : workers) {
                executor.submit(worker);
            }

            Thread.sleep(Duration.ofSeconds(config.durationSeconds));
            stopRequested.set(true);
            System.out.println("Duration elapsed, waiting for in-flight transactions to finish...");

            executor.shutdown();
            boolean finished = executor.awaitTermination(2L * Math.max(config.requestTimeoutSeconds, 10), TimeUnit.SECONDS);
            if (!finished) {
                System.err.println("Warning: not all stations finished cleanly within the grace period.");
            }
        }

        // Guard against a near-zero denominator producing Infinity/NaN in the
        // rate calculations below, which would otherwise emit invalid JSON.
        double wallClockSeconds = Math.max(0.001, (System.nanoTime() - startNanos) / 1_000_000_000.0);

        long totalTransactions = 0;
        long totalItems = 0;
        for (StationWorker worker : workers) {
            totalTransactions += worker.transactionsCompleted();
            totalItems += worker.itemsScanned();
        }

        System.out.println("Fetching low-stock alerts and popular items...");
        List<ApiClient.LowStockAlert> lowStock = safeFetchLowStock(client);
        List<ApiClient.PopularItem> popular = safeFetchPopular(client, config.popularLimit);

        ReportWriter.printConsoleReport(config, metrics, totalTransactions, totalItems, wallClockSeconds, lowStock, popular);

        try {
            var path = ReportWriter.writeJsonReport(config, metrics, totalTransactions, totalItems, wallClockSeconds, lowStock, popular);
            System.out.println();
            System.out.println("JSON report written to: " + path.toAbsolutePath());
        } catch (Exception e) {
            System.err.println("Failed to write JSON report: " + e.getMessage());
        }
    }

    private static List<ApiClient.LowStockAlert> safeFetchLowStock(ApiClient client) {
        try {
            return client.fetchLowStock();
        } catch (Exception e) {
            System.err.println("Could not fetch low-stock alerts: " + e.getMessage());
            return List.of();
        }
    }

    private static List<ApiClient.PopularItem> safeFetchPopular(ApiClient client, int limit) {
        try {
            return client.fetchPopularItems(limit);
        } catch (Exception e) {
            System.err.println("Could not fetch popular items: " + e.getMessage());
            return List.of();
        }
    }
}
