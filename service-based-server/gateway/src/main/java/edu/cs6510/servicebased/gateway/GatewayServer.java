package edu.cs6510.servicebased.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** 功能：提供单一 HTTP 入口，仅按路径转发请求；不承载业务规则或服务间调用。 */
public final class GatewayServer implements AutoCloseable {
    // 功能：这些首部只对一段连接有效，不能从客户端连接直接带到后端连接。
    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade");
    private static final Set<String> CLIENT_MANAGED = Set.of(
            "host", "content-length", "expect", "http2-settings");
    // 功能：小请求按实际长度分配；较大或未知长度保留流式增长读取，避免直接信任大长度首部。
    private static final int MAX_PREALLOCATED_BODY = 1024 * 1024;
    private static final byte[] EMPTY_BODY = new byte[0];
    private final String catalogBase;
    private final String checkoutBase;
    private final String analyticsBase;
    private final HttpServer server;
    private final ExecutorService requests;
    private final HttpClient client;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 功能：创建 HTTP 监听器、虚拟线程和可复用连接池，不立即开始接收请求。 */
    public GatewayServer(GatewayConfig config) throws IOException {
        // 功能：部署地址启动后不变，预先去尾斜线，避免每个请求重复编译正则。
        catalogBase = normalizeBase(config.catalogUrl());
        checkoutBase = normalizeBase(config.checkoutUrl());
        analyticsBase = normalizeBase(config.analyticsUrl());
        // 功能：关闭 JDK HttpServer 的 Nagle 合并，避免小 JSON 响应等待延迟 ACK；允许 JVM 参数覆盖。
        System.setProperty("sun.net.httpserver.nodelay", System.getProperty("sun.net.httpserver.nodelay", "true"));
        server = HttpServer.create(new InetSocketAddress(config.port()), 256);
        requests = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(requests);
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        server.createContext("/", this::handle);
    }

    /** 功能：启动入口；三个业务服务仍保持独立运行和独立部署。 */
    public void start() {
        server.start();
    }

    /** 功能：返回实际监听端口，允许测试让操作系统选择空闲端口。 */
    public int port() {
        return server.getAddress().getPort();
    }

    /** 功能：处理健康检查、路由查找及网络转发，并在结束时关闭本次交换资源。 */
    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getRawPath();
            if ("/health".equals(path)) {
                sendJson(exchange, 200, "{\"status\":\"UP\",\"service\":\"gateway\"}");
                return;
            }
            String backend = backendFor(path);
            if (backend == null) {
                sendJson(exchange, 404, "{\"error\":\"Unknown route\"}");
                return;
            }

            // 功能：等待对应服务的真实响应；网络故障返回明确的 502，超时返回 504。
            HttpResponse<byte[]> response;
            try {
                response = client.send(forwardRequest(exchange, backend), HttpResponse.BodyHandlers.ofByteArray());
            } catch (java.net.http.HttpTimeoutException e) {
                sendJson(exchange, 504, "{\"error\":\"Upstream timeout\"}");
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendJson(exchange, 503, "{\"error\":\"Gateway shutting down\"}");
                return;
            } catch (IOException | IllegalArgumentException e) {
                sendJson(exchange, 502, "{\"error\":\"Upstream unavailable\"}");
                return;
            }
            forwardResponse(exchange, response);
        }
    }

    /** 功能：按完整路径段匹配业务边界，防止 /items-other 等名称误入目录服务。 */
    private String backendFor(String path) {
        if (matches(path, "/items")) {
            return catalogBase;
        }
        if (matches(path, "/transactions") || matches(path, "/inventory")) {
            return checkoutBase;
        }
        return matches(path, "/analytics") ? analyticsBase : null;
    }

    /** 功能：仅处理已验证的上游根地址；字符串拼接继续保留客户端的原始编码路径。 */
    private static String normalizeBase(URI backend) {
        String base = backend.toString();
        return base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    }

    /** 功能：允许资源本身及其子路径，不进行含糊的字符串前缀路由。 */
    private static boolean matches(String path, String prefix) {
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    /** 功能：保留原始方法、编码路径、查询参数和请求体，同时重新生成连接相关首部。 */
    private HttpRequest forwardRequest(HttpExchange exchange, String backend) throws IOException {
        URI target = URI.create(backend + exchange.getRequestURI().toASCIIString());
        byte[] body = readRequestBody(exchange);
        HttpRequest.Builder builder = HttpRequest.newBuilder(target)
                .timeout(Duration.ofSeconds(60))
                .method(exchange.getRequestMethod(), body.length == 0
                        ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        Set<String> excluded = excludedHeaders(exchange.getRequestHeaders());
        excluded.addAll(CLIENT_MANAGED);
        exchange.getRequestHeaders().forEach((name, values) -> {
            if (!excluded.contains(name.toLowerCase(Locale.ROOT))) {
                values.forEach(value -> builder.header(name, value));
            }
        });
        return builder.build();
    }

    /** 功能：已知小正文精确读取，避免 JDK readAllBytes 为几十字节 JSON 先分配 16 KiB。 */
    private static byte[] readRequestBody(HttpExchange exchange) throws IOException {
        String declaredLength = exchange.getRequestHeaders().getFirst("Content-Length");
        if (declaredLength != null) {
            long length;
            try {
                length = Long.parseLong(declaredLength);
            } catch (NumberFormatException e) {
                throw new IOException("Invalid Content-Length", e);
            }
            if (length < 0) throw new IOException("Negative Content-Length");
            if (length <= MAX_PREALLOCATED_BODY) {
                var input = exchange.getRequestBody();
                byte[] body = length == 0 ? EMPTY_BODY : input.readNBytes((int) length);
                if (body.length != length) throw new EOFException("Incomplete request body");
                // 功能：确认本次正文边界并标记 EOF，让后续长连接请求不会被当成剩余正文。
                if (input.read() != -1) throw new IOException("Request body exceeds Content-Length");
                return body;
            }
        }
        // 功能：分块传输或超出预分配阈值时读取真实正文；阈值不是请求大小限制。
        return exchange.getRequestBody().readAllBytes();
    }

    /** 功能：返回上游真实状态、JSON 字节和端到端首部，不跟随或改写业务响应。 */
    private void forwardResponse(HttpExchange exchange, HttpResponse<byte[]> response) throws IOException {
        Set<String> excluded = excludedHeaders(response.headers().map());
        excluded.add("content-length");
        response.headers().map().forEach((name, values) -> {
            if (!excluded.contains(name.toLowerCase(Locale.ROOT))) {
                exchange.getResponseHeaders().put(name, List.copyOf(values));
            }
        });
        boolean head = "HEAD".equalsIgnoreCase(exchange.getRequestMethod());
        boolean noBody = head || response.statusCode() == 204 || response.statusCode() == 304;
        if (head) {
            response.headers().firstValue("content-length")
                    .ifPresent(value -> exchange.getResponseHeaders().set("Content-Length", value));
        }
        // 功能：按实际响应体长度重新计算 framing，保持客户端长连接可安全复用。
        byte[] body = response.body();
        exchange.sendResponseHeaders(response.statusCode(), noBody || body.length == 0 ? -1 : body.length);
        if (!noBody && body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    /** 功能：除标准逐跳首部外，也过滤 Connection 明确指定的自定义逐跳首部。 */
    private static Set<String> excludedHeaders(Map<String, List<String>> headers) {
        Set<String> excluded = new HashSet<>(HOP_BY_HOP);
        headers.forEach((name, values) -> {
            if ("connection".equalsIgnoreCase(name)) {
                values.forEach(value -> {
                    for (String token : value.split(",")) {
                        excluded.add(token.trim().toLowerCase(Locale.ROOT));
                    }
                });
            }
        });
        return excluded;
    }

    /** 功能：生成网关自己的健康或错误 JSON；健康只表示入口进程可响应。 */
    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        boolean head = "HEAD".equalsIgnoreCase(exchange.getRequestMethod());
        exchange.sendResponseHeaders(status, head ? -1 : body.length);
        if (!head) {
            exchange.getResponseBody().write(body);
        }
    }

    /** 功能：终止监听并释放连接池与虚拟线程；重复关闭不会重复清理。 */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            server.stop(1);
            client.close();
            requests.close();
        }
    }
}
