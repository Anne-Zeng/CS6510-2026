package edu.cs6510.servicebased.gateway;

import edu.cs6510.events.BusException;
import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** Real HTTP ingress against a recording MessageBus: no upstream HTTP server can mask direct calls. */
class GatewayServerTest {
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private volatile Function<Call, Reply> responder = call -> new Reply(200, "{\"ok\":true}");
    private GatewayServer gateway;
    private HttpClient client;

    @BeforeEach
    void startGateway() throws Exception {
        MessageBus bus = (MessageBus) Proxy.newProxyInstance(MessageBus.class.getClassLoader(),
                new Class<?>[]{MessageBus.class}, (proxy, method, args) -> {
                    if (!"request".equals(method.getName())) throw new UnsupportedOperationException(method.getName());
                    Call call = new Call((String) args[0], (String) args[1], (String) args[2], (String) args[3]);
                    calls.add(call);
                    return responder.apply(call);
                });
        gateway = new GatewayServer(new GatewayConfig(0), bus);
        gateway.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void closeGateway() {
        if (client != null) client.close();
        if (gateway != null) gateway.close();
    }

    @Test
    void mapsCatalogAndCheckoutRoutesToKafkaOperations() throws Exception {
        assertEquals(200, get("/items").statusCode());
        assertCall(0, "catalog", "GET_CATALOG", "{}");
        post("/transactions", "{\"stationId\":\"station-1\"}");
        assertCall(1, "checkout", "START_TRANSACTION", "{\"stationId\":\"station-1\"}");
        get("/transactions/tx-1");
        assertCall(2, "checkout", "GET_TRANSACTION", "{}");
        post("/transactions/tx-1/items", "{\"sku\":\"SKU-1\"}");
        assertCall(3, "checkout", "SCAN_ITEM", "{\"sku\":\"SKU-1\"}");
        post("/transactions/tx-1/complete", "");
        assertCall(4, "checkout", "COMPLETE_TRANSACTION", "{}");
        for (int i = 2; i <= 4; i++) assertEquals("tx-1", calls.get(i).key());
        assertNotEquals(calls.get(0).key(), calls.get(1).key());
    }

    @Test
    void relaysBusinessStatusAndUnicodeBodyWithoutChangingThem() throws Exception {
        String body = "{\"transactionId\":\"123\",\"name\":\"苹果🍎\"}";
        responder = call -> new Reply(201, body);
        HttpResponse<String> created = post("/transactions", "{\"stationId\":\"测试\"}");
        assertEquals(201, created.statusCode());
        assertEquals(body, created.body());
        assertEquals("{\"stationId\":\"测试\"}", calls.getFirst().body());
        responder = call -> new Reply(409, "{\"error\":\"Transaction already completed\"}");
        HttpResponse<String> conflict = post("/transactions/123/complete", "");
        assertEquals(409, conflict.statusCode());
        assertEquals("{\"error\":\"Transaction already completed\"}", conflict.body());
    }

    @Test
    void mapsOptionalQueryParametersAndChecksScanCheckpointBeforeAnalytics() throws Exception {
        responder = call -> "SCAN_CHECKPOINT".equals(call.operation())
                ? new Reply(200, "{\"totalScans\":724}") : new Reply(200, "{}");
        get("/inventory/low-stock?threshold=20");
        assertCall(0, "inventory", "GET_LOW_STOCK", "{\"threshold\":20}");
        get("/inventory/low-stock");
        assertNull(Json.read(calls.get(1).body(), Map.class).get("threshold"));
        get("/analytics/popular-items?limit=5");
        assertCall(2, "checkout", "SCAN_CHECKPOINT", "{}");
        assertCall(3, "analytics", "GET_POPULAR_ITEMS", "{\"limit\":5,\"minimumScans\":724}");
        get("/analytics/popular-items");
        Map<?, ?> query = Json.read(calls.get(5).body(), Map.class);
        assertNull(query.get("limit"));
        assertEquals(724, ((Number) query.get("minimumScans")).intValue());
    }

    @Test
    void checkpointFailureStopsAnalyticsQueryAndPreservesError() throws Exception {
        responder = call -> new Reply(503, "{\"error\":\"Checkout not ready\"}");
        HttpResponse<String> response = get("/analytics/popular-items");
        assertEquals(503, response.statusCode());
        assertEquals("{\"error\":\"Checkout not ready\"}", response.body());
        assertEquals(1, calls.size());
        assertEquals("SCAN_CHECKPOINT", calls.getFirst().operation());
    }

    @Test
    void doesNotReturnSuccessUntilBrokerReplyArrives() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch complete = new CountDownLatch(1);
        responder = call -> {
            received.countDown();
            try {
                if (!complete.await(5, TimeUnit.SECONDS)) throw new AssertionError("Test reply was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return new Reply(200, "{\"status\":\"COMPLETED\"}");
        };
        var response = client.sendAsync(HttpRequest.newBuilder(address("/transactions/tx-1/complete"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        try {
            assertTrue(received.await(3, TimeUnit.SECONDS));
            assertFalse(response.isDone(), "HTTP success must wait for committed business response");
        } finally {
            complete.countDown();
        }
        assertEquals("{\"status\":\"COMPLETED\"}", response.get(3, TimeUnit.SECONDS).body());
    }

    @Test
    void gatewayErrorsUseTheOpenApiErrorAndMessageFields() throws Exception {
        for (String path : List.of("/does-not-exist", "/inventory/low-stock?threshold=bad", "/transactions")) {
            var body = Json.read(get(path).body(), Map.class);
            assertEquals(java.util.Set.of("error", "message"), body.keySet());
            assertInstanceOf(String.class, body.get("error"));
            assertInstanceOf(String.class, body.get("message"));
        }
        responder = call -> { throw new BusException("timeout", new TimeoutException()); };
        assertEquals("GATEWAY_TIMEOUT", Json.read(get("/items").body(), Map.class).get("error"));
    }

    @Test
    void mapsBrokerTimeoutTo504AndOtherTransportFailuresTo502() throws Exception {
        responder = call -> { throw new BusException("Request timed out", new TimeoutException()); };
        assertEquals(504, get("/items").statusCode());
        responder = call -> { throw new BusException("Broker unavailable"); };
        assertEquals(502, get("/items").statusCode());
        responder = call -> new Reply(200, "{}");
        assertEquals(200, get("/items").statusCode());
    }

    @Test
    void rejectsInternalUnknownAndPathTraversalRoutesWithoutPublishing() throws Exception {
        for (String path : new String[]{"/", "/items-other", "/items/SKU-1", "/transactions2", "/transactions/",
                "/transactions/tx-1/unknown", "/transactions/tx-1/items/extra", "/inventory", "/analytics",
                "/internal/events/scans", "/internal/reservations/x", "/transactions/%2Fsecret",
                "/transactions/%2e%2e", "/analytics/../internal/events/scans",
                "/inventory/%2e%2e/internal/reservations/x"}) {
            assertEquals(404, get(path).statusCode(), path);
        }
        assertTrue(calls.isEmpty());
    }

    @Test
    void rejectsWrongMethodsAndInvalidNumericQueriesWithoutPublishing() throws Exception {
        assertEquals(405, get("/transactions").statusCode());
        assertEquals(405, get("/transactions/tx-1/items").statusCode());
        assertEquals(405, post("/items", "{}").statusCode());
        for (String query : new String[]{"threshold=foo", "threshold=", "threshold=1&threshold=2", "other=3",
                "threshold=2147483648"}) {
            assertEquals(400, get("/inventory/low-stock?" + query).statusCode(), query);
        }
        assertEquals(400, get("/analytics/popular-items?limit=foo").statusCode());
        assertTrue(calls.isEmpty());
    }

    @Test
    void healthIsLocalAndDoesNotRequireKafkaServiceReply() throws Exception {
        responder = call -> { throw new AssertionError("Health should not use message bus"); };
        assertEquals(200, get("/health").statusCode());
        assertEquals("{\"status\":\"UP\",\"service\":\"gateway\"}", get("/health").body());
        assertTrue(calls.isEmpty());
    }

    @Test
    void decodesTransactionIdWithoutTurningPathPlusIntoSpace() throws Exception {
        get("/transactions/abc%20def+ghi");
        assertEquals("abc def+ghi", calls.getFirst().key());
    }

    @Test
    void readsChunkedUnicodePostBodyBeforePublishing() throws Exception {
        String body = "{\"stationId\":\"" + "苹果🍎".repeat(1000) + "\"}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(address("/transactions"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(bytes))).build();
        assertEquals(200, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
        assertEquals(body, calls.getFirst().body());
    }

    @Test
    void rejectsTruncatedRequestBeforePublishing() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", gateway.port())) {
            socket.setSoTimeout(3000);
            String partial = "POST /transactions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 64\r\nConnection: close\r\n\r\nabc";
            socket.getOutputStream().write(partial.getBytes(StandardCharsets.US_ASCII));
            socket.shutdownOutput();
            assertTrue(new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII)
                    .startsWith("HTTP/1.1 400"));
        }
        assertTrue(calls.isEmpty());
    }

    @Test
    void validatesGatewayPortAndRejectsObsoleteHttpUpstreamOptions() {
        assertEquals(8080, GatewayConfig.fromArgs(new String[0]).port());
        assertEquals(8090, GatewayConfig.fromArgs(new String[]{"--port=8090"}).port());
        assertThrows(IllegalArgumentException.class, () -> GatewayConfig.fromArgs(new String[]{"--port=70000"}));
        assertThrows(IllegalArgumentException.class,
                () -> GatewayConfig.fromArgs(new String[]{"--catalog-url=http://localhost:8081"}));
    }

    private void assertCall(int index, String service, String operation, String body) {
        Call call = calls.get(index);
        assertEquals(service, call.service());
        assertEquals(operation, call.operation());
        assertEquals(body, call.body());
        assertNotNull(call.key());
        assertFalse(call.key().isBlank());
    }

    private URI address(String path) {
        return URI.create("http://127.0.0.1:" + gateway.port() + path);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(address(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(address(path)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private record Call(String service, String operation, String key, String body) {}
}
