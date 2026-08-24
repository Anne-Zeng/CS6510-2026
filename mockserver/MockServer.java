import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A minimal, in-memory reference implementation of the self-checkout API
 * contract (spec/self-checkout-openapi.yaml), built purely with the JDK's
 * built-in com.sun.net.httpserver - no external dependencies, matching the
 * load client's zero-dependency design.
 *
 * THIS IS NOT ONE OF THE WEEKLY ARCHITECTURE IMPLEMENTATIONS. It exists so
 * that (a) this project's own load client can be validated end-to-end
 * before any student code exists, and (b) students have a known-good
 * reference to compare their own week's server against while debugging.
 * It deliberately has no interesting architecture of its own: a handful of
 * ConcurrentHashMaps behind an HTTP server.
 *
 * Run: java --source 21 MockServer.java [port] [catalogSize] [stockPerItem] [lowStockThreshold]
 * Defaults: port=8080 catalogSize=2000 stockPerItem=10000 lowStockThreshold=50
 */
public final class MockServer {

    private static final int POPULARITY_WINDOW = 1000;

    record CatalogItem(String sku, String name, double price) {}

    static final class TxState {
        final String transactionId;
        final String stationId;
        final Deque<String> basket = new ConcurrentLinkedDeque<>();
        volatile String status = "OPEN"; // OPEN, COMPLETED, CANCELLED
        final Instant startedAt = Instant.now();

        TxState(String transactionId, String stationId) {
            this.transactionId = transactionId;
            this.stationId = stationId;
        }
    }

    private final List<CatalogItem> catalog;
    private final Map<String, CatalogItem> catalogBySku = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> stock = new ConcurrentHashMap<>();
    private final Map<String, TxState> transactions = new ConcurrentHashMap<>();
    private final AtomicLong txSequence = new AtomicLong();
    private final ConcurrentLinkedDeque<String> recentScans = new ConcurrentLinkedDeque<>();
    private final int lowStockThreshold;

    public MockServer(int catalogSize, int stockPerItem, int lowStockThreshold) {
        this.lowStockThreshold = lowStockThreshold;
        this.catalog = new java.util.ArrayList<>(catalogSize);
        for (int i = 1; i <= catalogSize; i++) {
            String sku = "SKU-" + String.format("%06d", i);
            String name = "Item " + i;
            double price = 0.5 + (i % 47) * 0.35; // arbitrary but deterministic spread
            CatalogItem item = new CatalogItem(sku, name, Math.round(price * 100.0) / 100.0);
            catalog.add(item);
            catalogBySku.put(sku, item);
            stock.put(sku, new AtomicInteger(stockPerItem));
        }
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        int catalogSize = args.length > 1 ? Integer.parseInt(args[1]) : 2000;
        int stockPerItem = args.length > 2 ? Integer.parseInt(args[2]) : 10000;
        int lowStockThreshold = args.length > 3 ? Integer.parseInt(args[3]) : 50;

        MockServer server = new MockServer(catalogSize, stockPerItem, lowStockThreshold);
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/items", server::handleItems);
        http.createContext("/transactions", server::handleTransactions);
        http.createContext("/inventory/low-stock", server::handleLowStock);
        http.createContext("/analytics/popular-items", server::handlePopularItems);
        http.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        http.start();
        System.out.println("Mock self-checkout server listening on port " + port);
        System.out.println("Catalog size=" + catalogSize + " stockPerItem=" + stockPerItem + " lowStockThreshold=" + lowStockThreshold);
    }

    // ---------------------------------------------------------------
    // /items
    // ---------------------------------------------------------------

    private void handleItems(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"METHOD_NOT_ALLOWED\",\"message\":\"Use GET\"}");
            return;
        }
        StringBuilder sb = new StringBuilder("{\"items\":[");
        for (int i = 0; i < catalog.size(); i++) {
            CatalogItem item = catalog.get(i);
            sb.append("{\"sku\":\"").append(item.sku()).append("\",\"name\":\"").append(item.name())
                    .append("\",\"price\":").append(item.price()).append("}");
            if (i < catalog.size() - 1) sb.append(",");
        }
        sb.append("]}");
        respond(ex, 200, sb.toString());
    }

    // ---------------------------------------------------------------
    // /transactions and sub-paths
    // ---------------------------------------------------------------

    private static final Pattern ITEMS_PATH = Pattern.compile("^/transactions/([^/]+)/items$");
    private static final Pattern COMPLETE_PATH = Pattern.compile("^/transactions/([^/]+)/complete$");
    private static final Pattern STATUS_PATH = Pattern.compile("^/transactions/([^/]+)$");

    private void handleTransactions(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String method = ex.getRequestMethod();

        if (path.equals("/transactions") && "POST".equals(method)) {
            startTransaction(ex);
            return;
        }
        Matcher itemsMatcher = ITEMS_PATH.matcher(path);
        if (itemsMatcher.matches() && "POST".equals(method)) {
            scanItem(ex, urlDecode(itemsMatcher.group(1)));
            return;
        }
        Matcher completeMatcher = COMPLETE_PATH.matcher(path);
        if (completeMatcher.matches() && "POST".equals(method)) {
            completeTransaction(ex, urlDecode(completeMatcher.group(1)));
            return;
        }
        Matcher statusMatcher = STATUS_PATH.matcher(path);
        if (statusMatcher.matches() && "GET".equals(method)) {
            getStatus(ex, urlDecode(statusMatcher.group(1)));
            return;
        }
        respond(ex, 404, "{\"error\":\"NOT_FOUND\",\"message\":\"No such route\"}");
    }

    private void startTransaction(HttpExchange ex) throws IOException {
        Map<String, Object> body = Json.parseObject(readBody(ex));
        String stationId = Json.getString(body, "stationId");
        if (stationId == null || stationId.isBlank()) {
            respond(ex, 400, "{\"error\":\"INVALID_REQUEST\",\"message\":\"stationId is required\"}");
            return;
        }
        String txId = "tx-" + txSequence.incrementAndGet();
        transactions.put(txId, new TxState(txId, stationId));
        respond(ex, 201, "{\"transactionId\":\"" + txId + "\",\"stationId\":\"" + stationId
                + "\",\"status\":\"OPEN\",\"itemCount\":0,\"runningTotal\":0.0,\"startedAt\":\""
                + Instant.now() + "\"}");
    }

    private void scanItem(HttpExchange ex, String txId) throws IOException {
        TxState tx = transactions.get(txId);
        if (tx == null) {
            respond(ex, 404, "{\"error\":\"NOT_FOUND\",\"message\":\"No such transaction\"}");
            return;
        }
        if (!"OPEN".equals(tx.status)) {
            respond(ex, 409, "{\"error\":\"TRANSACTION_NOT_OPEN\",\"message\":\"Transaction is not open\"}");
            return;
        }
        Map<String, Object> body = Json.parseObject(readBody(ex));
        String sku = Json.getString(body, "sku");
        CatalogItem item = sku == null ? null : catalogBySku.get(sku);
        if (item == null) {
            respond(ex, 404, "{\"error\":\"UNKNOWN_SKU\",\"message\":\"No such SKU\"}");
            return;
        }
        tx.basket.add(sku);
        recentScans.addLast(sku);
        while (recentScans.size() > POPULARITY_WINDOW) recentScans.pollFirst();

        int itemCount = tx.basket.size();
        double runningTotal = tx.basket.stream().mapToDouble(s -> catalogBySku.get(s).price()).sum();
        respond(ex, 200, "{\"transactionId\":\"" + txId + "\",\"sku\":\"" + sku + "\",\"name\":\"" + item.name()
                + "\",\"unitPrice\":" + item.price() + ",\"itemCount\":" + itemCount
                + ",\"runningTotal\":" + round2(runningTotal) + "}");
    }

    private void completeTransaction(HttpExchange ex, String txId) throws IOException {
        TxState tx = transactions.get(txId);
        if (tx == null) {
            respond(ex, 404, "{\"error\":\"NOT_FOUND\",\"message\":\"No such transaction\"}");
            return;
        }
        if (!"OPEN".equals(tx.status)) {
            respond(ex, 409, "{\"error\":\"TRANSACTION_NOT_OPEN\",\"message\":\"Transaction already finalized\"}");
            return;
        }
        if (tx.basket.isEmpty()) {
            respond(ex, 409, "{\"error\":\"EMPTY_BASKET\",\"message\":\"Cannot complete an empty transaction\"}");
            return;
        }
        tx.status = "COMPLETED";

        // Decrement stock per unit, never going below zero. A CAS loop here
        // is the "correct" version of the exact operation that naive
        // check-then-decrement implementations get wrong under concurrency
        // in the distributed weeks of the course.
        double total = 0.0;
        for (String sku : tx.basket) {
            AtomicInteger s = stock.get(sku);
            s.updateAndGet(current -> Math.max(0, current - 1));
            total += catalogBySku.get(sku).price();
        }

        respond(ex, 200, "{\"transactionId\":\"" + txId + "\",\"stationId\":\"" + tx.stationId
                + "\",\"itemCount\":" + tx.basket.size() + ",\"totalAmount\":" + round2(total)
                + ",\"startedAt\":\"" + tx.startedAt + "\",\"completedAt\":\"" + Instant.now()
                + "\",\"lines\":[]}");
    }

    private void getStatus(HttpExchange ex, String txId) throws IOException {
        TxState tx = transactions.get(txId);
        if (tx == null) {
            respond(ex, 404, "{\"error\":\"NOT_FOUND\",\"message\":\"No such transaction\"}");
            return;
        }
        double runningTotal = tx.basket.stream().mapToDouble(s -> catalogBySku.get(s).price()).sum();
        respond(ex, 200, "{\"transactionId\":\"" + txId + "\",\"stationId\":\"" + tx.stationId
                + "\",\"status\":\"" + tx.status + "\",\"itemCount\":" + tx.basket.size()
                + ",\"runningTotal\":" + round2(runningTotal) + ",\"startedAt\":\"" + tx.startedAt + "\"}");
    }

    // ---------------------------------------------------------------
    // /inventory/low-stock
    // ---------------------------------------------------------------

    private void handleLowStock(HttpExchange ex) throws IOException {
        StringBuilder sb = new StringBuilder("{\"threshold\":" + lowStockThreshold
                + ",\"generatedAt\":\"" + Instant.now() + "\",\"alerts\":[");
        boolean first = true;
        for (CatalogItem item : catalog) {
            int current = stock.get(item.sku()).get();
            if (current < lowStockThreshold) {
                if (!first) sb.append(",");
                sb.append("{\"sku\":\"").append(item.sku()).append("\",\"name\":\"").append(item.name())
                        .append("\",\"currentStock\":").append(current)
                        .append(",\"threshold\":").append(lowStockThreshold)
                        .append(",\"triggeredAt\":\"").append(Instant.now()).append("\"}");
                first = false;
            }
        }
        sb.append("]}");
        respond(ex, 200, sb.toString());
    }

    // ---------------------------------------------------------------
    // /analytics/popular-items
    // ---------------------------------------------------------------

    private void handlePopularItems(HttpExchange ex) throws IOException {
        int limit = 10;
        String query = ex.getRequestURI().getQuery();
        if (query != null && query.startsWith("limit=")) {
            try {
                limit = Integer.parseInt(query.substring("limit=".length()));
            } catch (NumberFormatException ignored) {
                // fall back to default
            }
        }

        // NOTE: this reference server always computes popularity over
        // whatever the current last-POPULARITY_WINDOW scans are (a true
        // sliding window), recomputed fresh on every request. The full
        // hopping-window semantics (recompute every slideInterval scans,
        // report windowStart/windowEnd) are left as part of the exercise
        // for each week's real implementation - this mock only needs to be
        // good enough to validate the client's parsing and request shape.
        Map<String, Long> counts = new ConcurrentHashMap<>();
        List<String> snapshot = List.copyOf(recentScans);
        for (String sku : snapshot) {
            counts.merge(sku, 1L, Long::sum);
        }
        List<Map.Entry<String, Long>> ranked = counts.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(limit)
                .toList();

        StringBuilder sb = new StringBuilder();
        sb.append("{\"windowSize\":").append(POPULARITY_WINDOW)
                .append(",\"slideInterval\":500")
                .append(",\"windowStart\":").append(Math.max(0, txSequence.get() - snapshot.size()))
                .append(",\"windowEnd\":").append(txSequence.get())
                .append(",\"computedAt\":\"").append(Instant.now()).append("\"")
                .append(",\"items\":[");
        for (int i = 0; i < ranked.size(); i++) {
            Map.Entry<String, Long> e = ranked.get(i);
            CatalogItem item = catalogBySku.get(e.getKey());
            sb.append("{\"sku\":\"").append(e.getKey()).append("\",\"name\":\"").append(item.name())
                    .append("\",\"scanCount\":").append(e.getValue())
                    .append(",\"rank\":").append(i + 1).append("}");
            if (i < ranked.size() - 1) sb.append(",");
        }
        sb.append("]}");
        respond(ex, 200, sb.toString());
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static String readBody(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static String urlDecode(String s) {
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
