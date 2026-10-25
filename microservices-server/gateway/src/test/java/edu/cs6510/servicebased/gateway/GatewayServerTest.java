package edu.cs6510.servicebased.gateway;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/** 功能：使用真实的临时 HTTP 服务验证代理行为，HTTP 路由、请求透传和错误响应等。 */
class GatewayServerTest {
    private final Map<String, ObservedRequest> received = new ConcurrentHashMap<>();
    private HttpServer catalog;
    private HttpServer checkout;
    private HttpServer analytics;
    private HttpServer inventory;
    private GatewayServer gateway;
    private HttpClient client;

    /** 功能：每个测试启动四个隔离的模拟业务服务及真实网关，端口由系统自动分配。 */
    @BeforeEach
    void startServers() throws IOException {
        catalog = backend("catalog");// 创建四个模拟后端。
        checkout = backend("checkout");
        analytics = backend("analytics");
        inventory = backend("inventory");
        // 创建真实网关，让它把请求转发给这四个模拟后端。功能：同时覆盖带尾斜线和不带尾斜线的部署根地址，原有路由测试检查结果不变。
        gateway = new GatewayServer(new GatewayConfig(0, URI.create(address(catalog) + "/"),
                address(checkout), URI.create(address(analytics) + "/"), address(inventory)));
        gateway.start();
        client = HttpClient.newHttpClient();
    }

    /** 功能：关闭所有网络资源，保证测试之间不共享监听端口或长连接。 */
    @AfterEach
    void stopServers() {
        if (client != null) client.close();
        if (gateway != null) gateway.close();
        if (catalog != null) catalog.stop(0);
        if (checkout != null) checkout.stop(0);
        if (analytics != null) analytics.stop(0);
        if (inventory != null) inventory.stop(0);
    }

    /** 功能：验证目录、交易、库存和分析请求确实分别到达正确的独立服务/后端。 */
    @Test
    void routesAllBusinessResources() throws Exception {
        assertEquals("catalog", get("/items").headers().firstValue("X-Upstream").orElseThrow());
        assertEquals("catalog", get("/items/SKU-1").headers().firstValue("X-Upstream").orElseThrow());
        assertEquals("checkout", get("/transactions/1").headers().firstValue("X-Upstream").orElseThrow());
        assertEquals("inventory", get("/inventory/low-stock").headers().firstValue("X-Upstream").orElseThrow());
        assertEquals("analytics", get("/analytics/popular-items").headers().firstValue("X-Upstream").orElseThrow());
    }

    /** 功能：验证原始编码路径、重复查询参数、POST JSON 和上游 201 状态都不被改写。 */
    @Test
    void preservesMethodEncodedUriBodyAndStatus() throws Exception {
        String path = "/transactions/abc%20def/items?sku=SKU%2F7&x=1&x=2";
        String body = "{\"sku\":\"SKU/7\",\"quantity\":2,\"name\":\"苹果\"}";
        HttpRequest request = HttpRequest.newBuilder(gatewayAddress(path))
                .header("Content-Type", "application/json")
                .header("X-Request-Id", "checkout-test")
                .header("Proxy-Authorization", "must-not-forward")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        ObservedRequest actual = received.get("checkout");
        assertEquals(201, response.statusCode());
        assertEquals(body, response.body());
        assertEquals("POST", actual.method());
        assertEquals("/transactions/abc%20def/items", actual.path());
        assertEquals("sku=SKU%2F7&x=1&x=2", actual.query());
        assertEquals(body, actual.body());
        assertEquals("checkout-test", actual.headers().getFirst("X-Request-Id"));
        assertNull(actual.headers().getFirst("Proxy-Authorization"));
        assertEquals("application/json", response.headers().firstValue("Content-Type").orElseThrow());
    }

    /** 功能：分块上传没有已知 Content-Length；验证多字节正文完整透传及后续请求仍正常。 */
    @Test
    void forwardsChunkedUnicodeBodyWithoutChangingBytes() throws Exception {
        String body = "分块正文🍎".repeat(8000);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(gatewayAddress("/transactions/chunked"))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(bytes))).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());
        assertEquals(body, response.body());
        assertEquals(body, received.get("checkout").body());
        assertEquals(Integer.toString(bytes.length), received.get("checkout").headers().getFirst("Content-Length"));
        assertEquals(200, get("/items").statusCode());
    }

    /** 功能：大于预分配阈值的已知长度正文仍完整通过，阈值不会变成接口大小限制。 */
    @Test
    void forwardsLargeKnownLengthBodyThroughFallback() throws Exception {
        String body = "x".repeat(1_100_000);
        HttpRequest request = HttpRequest.newBuilder(gatewayAddress("/transactions/large"))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());
        assertEquals(body, response.body());
        assertEquals(body, received.get("checkout").body());
        assertEquals("1100000", received.get("checkout").headers().getFirst("Content-Length"));
    }

    /** 功能：声明长度后提前结束上传不能被截断成成功请求，也不能破坏网关后续处理。 */
    @Test
    void rejectsTruncatedKnownLengthBodyBeforeContactingBackend() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", gateway.port())) {
            socket.setSoTimeout(5000);
            String partial = "POST /transactions/truncated HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\nContent-Length: 64\r\nConnection: close\r\n\r\nabc";
            socket.getOutputStream().write(partial.getBytes(StandardCharsets.US_ASCII));
            socket.shutdownOutput();
            String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
            assertTrue(response.startsWith("HTTP/1.1 502"), response);
            assertNull(received.get("checkout"));
        }
        assertEquals(200, get("/items").statusCode());
    }

    /** 功能：验证 Connection 声明的自定义逐跳首部不会泄漏到入口客户端。 */
    @Test
    void removesHopByHopResponseHeaders() throws Exception {
        HttpResponse<String> response = get("/items");
        assertTrue(response.headers().firstValue("X-Private-Hop").isEmpty());
        assertEquals("catalog", response.headers().firstValue("X-Upstream").orElseThrow());
    }

    /** 功能：验证未知路径和相似前缀均返回 404，且不会触发任一业务服务。 */
    @Test
    void rejectsUnknownRoutesAtTheGateway() throws Exception {
        for (String path : new String[]{"/", "/items-other", "/transactions2", "/analytics-other", "/items%2Fsecret", "/inventory-other", "/internal/events/scans", "/internal/reservations/x", "/analytics/../internal/events/scans",
                "/analytics/%2e%2e/internal/events/scans", "/analytics/..;x/internal/events/scans",
                "/inventory/%2e%2e/internal/reservations/x"}) {
            assertEquals(404, get(path).statusCode(), path);
        }
        assertTrue(received.isEmpty());
    }

    /** 功能：验证健康接口只检查入口进程，不向数据库或业务服务发送探测请求。 */
    @Test
    void exposesLocalGatewayHealth() throws Exception {
        HttpResponse<String> response = get("/health");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"service\":\"gateway\""));
        assertTrue(received.isEmpty());
    }

    /** 功能：验证服务下线时得到明确的 502，同时其他业务路由仍可正常使用。 */
    @Test
    void reportsBackendFailureWithoutBreakingOtherRoutes() throws Exception {
        checkout.stop(0);
        assertEquals(502, get("/transactions").statusCode());
        assertEquals(200, get("/items").statusCode());
    }

    /** 功能：Inventory 下线只影响库存路由，不能错误回退到 Checkout。 */
    @Test
    void inventoryFailureDoesNotRouteToCheckout() throws Exception {
        inventory.stop(0);
        assertEquals(502, get("/inventory/low-stock").statusCode());
        assertEquals(200, get("/transactions/1").statusCode());
        assertEquals(200, get("/analytics/popular-items").statusCode());
    }

    /** 功能：验证无正文状态和 HEAD 响应不会破坏 HTTP 报文边界及下一次请求。 */
    @Test
    void handlesNoContentAndHeadWithPersistentConnections() throws Exception {
        HttpResponse<String> noContent = get("/transactions/empty");
        assertEquals(204, noContent.statusCode());
        assertEquals("", noContent.body());
        HttpRequest headRequest = HttpRequest.newBuilder(gatewayAddress("/items"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<String> head = client.send(headRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, head.statusCode());
        assertEquals("", head.body());
        assertEquals("17", head.headers().firstValue("Content-Length").orElseThrow());
        assertEquals(200, get("/items").statusCode());
    }

    /** 功能：验证部署命令的默认值、自定义地址和配置错误处理。 */
    @Test
    void validatesCommandLineConfiguration() {
        GatewayConfig defaults = GatewayConfig.fromArgs(new String[0]);
        assertEquals(8080, defaults.port());
        assertEquals(8084, defaults.inventoryUrl().getPort());
        GatewayConfig custom = GatewayConfig.fromArgs(new String[]{
                "--port=8090", "--catalog-url=http://localhost:9011/",
                "--checkout-url=http://localhost:9012", "--analytics-url=http://localhost:9013", "--inventory-url=http://localhost:9014"});
        assertEquals(8090, custom.port());
        assertEquals(9011, custom.catalogUrl().getPort());
        assertEquals(9012, custom.checkoutUrl().getPort());
        assertEquals(9013, custom.analyticsUrl().getPort());
        assertEquals(9014, custom.inventoryUrl().getPort());
        assertThrows(IllegalArgumentException.class,
                () -> GatewayConfig.fromArgs(new String[]{"--inventory-url=http://localhost/private"}));
        assertThrows(IllegalArgumentException.class, () -> GatewayConfig.fromArgs(new String[]{"--unknown=1"}));
        assertThrows(IllegalArgumentException.class, () -> GatewayConfig.fromArgs(new String[]{"--port=70000"}));
        assertThrows(IllegalArgumentException.class,
                () -> GatewayConfig.fromArgs(new String[]{"--catalog-url=http://localhost/base"}));
    }

    /** 功能：创建一个简单的 HTTP 服务器，记录收到的请求，并返回带有后端名称的响应。 */
    private HttpServer backend(String name) throws IOException {
        HttpServer backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        backend.createContext("/", exchange -> {
            try (exchange) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                received.put(name, new ObservedRequest(exchange.getRequestMethod(),
                        exchange.getRequestURI().getRawPath(), exchange.getRequestURI().getRawQuery(),
                        body, exchange.getRequestHeaders()));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("X-Upstream", name);
                exchange.getResponseHeaders().set("Connection", "X-Private-Hop");
                exchange.getResponseHeaders().set("X-Private-Hop", "local-only");
                if (exchange.getRequestURI().getPath().endsWith("/empty")) {
                    exchange.sendResponseHeaders(204, -1);
                } else if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Content-Length", "17");
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    byte[] reply = (body.isEmpty() ? "{\"source\":\"" + name + "\"}" : body)
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders("POST".equals(exchange.getRequestMethod()) ? 201 : 200, reply.length);
                    exchange.getResponseBody().write(reply);
                }
            }
        });
        backend.start();
        return backend;
    }

    /** 功能：将临时监听端口转换为供网关使用的根地址。 */
    private URI address(HttpServer backend) {
        return URI.create("http://127.0.0.1:" + backend.getAddress().getPort());
    }

    /** 功能：为当前网关构造保留原始编码的测试请求地址。 */
    private URI gatewayAddress(String path) {
        return URI.create("http://127.0.0.1:" + gateway.port() + path);
    }

    /** 功能：通过真实 TCP 连接执行 GET，而不是直接调用网关内部方法。 */
    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(gatewayAddress(path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** 功能：保存模拟服务收到的数据，供端到端断言使用。 */
    private record ObservedRequest(String method, String path, String query, String body, Headers headers) {
    }
}
