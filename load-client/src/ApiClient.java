import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Thin wrapper over java.net.http.HttpClient implementing exactly the
 * self-checkout OpenAPI contract (see spec/self-checkout-openapi.yaml).
 * Deliberately has no knowledge of *how* the server is architected - it only
 * knows the HTTP contract, which is what makes it reusable, unmodified,
 * across every week's implementation.
 */
public final class ApiClient {

    /** Thrown for any non-2xx response, or a transport-level failure. */
    public static final class ApiException extends RuntimeException {
        public final int statusCode; // -1 for transport-level failures (timeouts, connection refused, etc.)

        public ApiException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }
    }

    private final String baseUrl;
    private final HttpClient http;
    private final Duration requestTimeout;

    public ApiClient(String baseUrl, Duration requestTimeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public List<CatalogItem> fetchCatalog() {
        Map<String, Object> body = getJson("/items");
        List<Object> rawItems = Json.getList(body, "items");
        List<CatalogItem> items = new ArrayList<>(rawItems.size());
        for (Object o : rawItems) {
            Map<String, Object> item = Json.asObject(o);
            items.add(new CatalogItem(
                    Json.getString(item, "sku"),
                    Json.getString(item, "name"),
                    Json.getDouble(item, "price")));
        }
        return items;
    }

    public record StartedTransaction(String transactionId, String stationId, String status) {}

    public StartedTransaction startTransaction(String stationId) {
        String requestBody = "{\"stationId\":" + Json.quote(stationId) + "}";
        Map<String, Object> body = postJson("/transactions", requestBody, 201);
        return new StartedTransaction(
                Json.getString(body, "transactionId"),
                Json.getString(body, "stationId"),
                Json.getString(body, "status"));
    }

    public record ScanOutcome(String sku, double unitPrice, int itemCount, double runningTotal) {}

    public ScanOutcome scanItem(String transactionId, String sku) {
        String requestBody = "{\"sku\":" + Json.quote(sku) + "}";
        Map<String, Object> body = postJson("/transactions/" + enc(transactionId) + "/items", requestBody, 200);
        return new ScanOutcome(
                Json.getString(body, "sku"),
                Json.getDouble(body, "unitPrice"),
                Json.getInt(body, "itemCount"),
                Json.getDouble(body, "runningTotal"));
    }

    public record ReceiptSummary(int itemCount, double totalAmount) {}

    public ReceiptSummary completeTransaction(String transactionId) {
        Map<String, Object> body = postJson("/transactions/" + enc(transactionId) + "/complete", "{}", 200);
        return new ReceiptSummary(
                Json.getInt(body, "itemCount"),
                Json.getDouble(body, "totalAmount"));
    }

    public record LowStockAlert(String sku, String name, int currentStock, int threshold) {}

    public List<LowStockAlert> fetchLowStock() {
        Map<String, Object> body = getJson("/inventory/low-stock");
        List<Object> raw = Json.getList(body, "alerts");
        List<LowStockAlert> alerts = new ArrayList<>(raw.size());
        for (Object o : raw) {
            Map<String, Object> a = Json.asObject(o);
            alerts.add(new LowStockAlert(
                    Json.getString(a, "sku"),
                    Json.getString(a, "name"),
                    Json.getInt(a, "currentStock"),
                    Json.getInt(a, "threshold")));
        }
        return alerts;
    }

    public record PopularItem(String sku, String name, int scanCount, int rank) {}

    public List<PopularItem> fetchPopularItems(int limit) {
        Map<String, Object> body = getJson("/analytics/popular-items?limit=" + limit);
        List<Object> raw = Json.getList(body, "items");
        List<PopularItem> items = new ArrayList<>(raw.size());
        for (Object o : raw) {
            Map<String, Object> p = Json.asObject(o);
            items.add(new PopularItem(
                    Json.getString(p, "sku"),
                    Json.getString(p, "name"),
                    Json.getInt(p, "scanCount"),
                    Json.getInt(p, "rank")));
        }
        return items;
    }

    // ---------------------------------------------------------------

    private Map<String, Object> getJson(String path) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        return send(request, 200);
    }

    private Map<String, Object> postJson(String path, String jsonBody, int expectedStatus) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return send(request, expectedStatus);
    }

    private Map<String, Object> send(HttpRequest request, int expectedStatus) {
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new ApiException("Transport error calling " + request.uri() + ": " + e.getMessage(), -1);
        }
        if (response.statusCode() != expectedStatus) {
            throw new ApiException(
                    "Unexpected status " + response.statusCode() + " from " + request.uri() + ": " + response.body(),
                    response.statusCode());
        }
        if (response.body() == null || response.body().isBlank()) {
            return Map.of();
        }
        return Json.parseObject(response.body());
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }
}
