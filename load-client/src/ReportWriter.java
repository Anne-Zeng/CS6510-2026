import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Prints the end-of-run console report and writes a matching JSON file to disk. */
public final class ReportWriter {

    public static void printConsoleReport(Config config, Metrics metrics, long totalTransactions, long totalItems,
                                           double wallClockSeconds,
                                           List<ApiClient.LowStockAlert> lowStock,
                                           List<ApiClient.PopularItem> popular) {
        System.out.println();
        System.out.println("================ LOAD TEST REPORT ================");
        System.out.printf("Base URL:        %s%n", config.baseUrl);
        System.out.printf("Stations:        %d%n", config.stations);
        System.out.printf("Duration:        %.1fs (requested %ds)%n", wallClockSeconds, config.durationSeconds);
        System.out.printf("Transactions:    %d (%.1f/sec)%n", totalTransactions, totalTransactions / wallClockSeconds);
        System.out.printf("Items scanned:   %d (%.1f/sec)%n", totalItems, totalItems / wallClockSeconds);
        System.out.println();
        System.out.printf("%-22s %8s %8s %10s %10s %10s %10s %10s %8s%n",
                "Operation", "OK", "Errors", "Mean(ms)", "P50(ms)", "P95(ms)", "P99(ms)", "Max(ms)", "Err%");
        for (Metrics.Operation op : Metrics.Operation.values()) {
            Metrics.Summary s = metrics.summarize(op);
            System.out.printf("%-22s %8d %8d %10.2f %10.2f %10.2f %10.2f %10.2f %7.2f%%%n",
                    s.operation(), s.successCount(), s.errorCount(), s.meanMs(), s.p50Ms(), s.p95Ms(), s.p99Ms(),
                    s.maxMs(), s.errorRate() * 100.0);
        }

        System.out.println();
        System.out.println("Low-stock alerts (" + lowStock.size() + "):");
        if (lowStock.isEmpty()) {
            System.out.println("  (none)");
        } else {
            for (ApiClient.LowStockAlert a : lowStock) {
                System.out.printf("  %-12s %-30s stock=%-6d threshold=%d%n", a.sku(), a.name(), a.currentStock(), a.threshold());
            }
        }

        System.out.println();
        System.out.println("Most popular items (top " + popular.size() + "):");
        if (popular.isEmpty()) {
            System.out.println("  (none)");
        } else {
            for (ApiClient.PopularItem p : popular) {
                System.out.printf("  #%-3d %-12s %-30s scans=%d%n", p.rank(), p.sku(), p.name(), p.scanCount());
            }
        }
        System.out.println("===================================================");
    }

    public static Path writeJsonReport(Config config, Metrics metrics, long totalTransactions, long totalItems,
                                        double wallClockSeconds,
                                        List<ApiClient.LowStockAlert> lowStock,
                                        List<ApiClient.PopularItem> popular) throws IOException {
        Path dir = Path.of(config.reportDir);
        Files.createDirectories(dir);
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(java.time.LocalDateTime.now());
        Path file = dir.resolve("report-" + timestamp + ".json");

        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"generatedAt\": ").append(Json.quote(Instant.now().toString())).append(",\n");
        json.append("  \"config\": {\n");
        json.append("    \"baseUrl\": ").append(Json.quote(config.baseUrl)).append(",\n");
        json.append("    \"stations\": ").append(config.stations).append(",\n");
        json.append("    \"durationSecondsRequested\": ").append(config.durationSeconds).append(",\n");
        json.append("    \"minItems\": ").append(config.minItems).append(",\n");
        json.append("    \"maxItems\": ").append(config.maxItems).append("\n");
        json.append("  },\n");
        json.append("  \"wallClockSeconds\": ").append(wallClockSeconds).append(",\n");
        json.append("  \"totalTransactions\": ").append(totalTransactions).append(",\n");
        json.append("  \"totalItemsScanned\": ").append(totalItems).append(",\n");
        json.append("  \"transactionsPerSecond\": ").append(totalTransactions / wallClockSeconds).append(",\n");
        json.append("  \"itemsPerSecond\": ").append(totalItems / wallClockSeconds).append(",\n");

        json.append("  \"operations\": [\n");
        Metrics.Operation[] ops = Metrics.Operation.values();
        for (int i = 0; i < ops.length; i++) {
            Metrics.Summary s = metrics.summarize(ops[i]);
            json.append("    {\n");
            json.append("      \"operation\": ").append(Json.quote(s.operation())).append(",\n");
            json.append("      \"successCount\": ").append(s.successCount()).append(",\n");
            json.append("      \"errorCount\": ").append(s.errorCount()).append(",\n");
            json.append("      \"errorRate\": ").append(s.errorRate()).append(",\n");
            json.append("      \"meanMs\": ").append(s.meanMs()).append(",\n");
            json.append("      \"p50Ms\": ").append(s.p50Ms()).append(",\n");
            json.append("      \"p95Ms\": ").append(s.p95Ms()).append(",\n");
            json.append("      \"p99Ms\": ").append(s.p99Ms()).append(",\n");
            json.append("      \"minMs\": ").append(s.minMs()).append(",\n");
            json.append("      \"maxMs\": ").append(s.maxMs()).append("\n");
            json.append("    }").append(i < ops.length - 1 ? ",\n" : "\n");
        }
        json.append("  ],\n");

        json.append("  \"lowStockAlerts\": [\n");
        for (int i = 0; i < lowStock.size(); i++) {
            ApiClient.LowStockAlert a = lowStock.get(i);
            json.append("    {\"sku\": ").append(Json.quote(a.sku()))
                    .append(", \"name\": ").append(Json.quote(a.name()))
                    .append(", \"currentStock\": ").append(a.currentStock())
                    .append(", \"threshold\": ").append(a.threshold())
                    .append("}").append(i < lowStock.size() - 1 ? ",\n" : "\n");
        }
        json.append("  ],\n");

        json.append("  \"popularItems\": [\n");
        for (int i = 0; i < popular.size(); i++) {
            ApiClient.PopularItem p = popular.get(i);
            json.append("    {\"rank\": ").append(p.rank())
                    .append(", \"sku\": ").append(Json.quote(p.sku()))
                    .append(", \"name\": ").append(Json.quote(p.name()))
                    .append(", \"scanCount\": ").append(p.scanCount())
                    .append("}").append(i < popular.size() - 1 ? ",\n" : "\n");
        }
        json.append("  ]\n");
        json.append("}\n");

        Files.writeString(file, json.toString());
        return file;
    }
}
