package edu.cs6510.servicebased.gateway;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.cs6510.events.BusException;
import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import java.util.concurrent.atomic.AtomicBoolean;

//接收 HTTP请求、匹配路由、转换为 Kafka 请求，再将响应转换回 HTTP
/** Public synchronous HTTP ingress. Every business operation crosses Kafka before returning a response.
 * Each keepalive connection runs on a virtual thread; the gateway has no business state or database. */
public final class GatewayServer implements AutoCloseable {
    private static final int CLIENT_IDLE_MILLIS = 30_000;
    private static final String UNKNOWN_ROUTE = errorBody("NOT_FOUND", "Unknown route");
    private static final String UPSTREAM_UNAVAILABLE = errorBody("SERVICE_UNAVAILABLE", "Message service unavailable");
    private final MessageBus bus;
    private final ServerSocket listener;
    private final Thread acceptor;
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Injecting a bus lets routing and failure behavior be tested without a broker. */
    public GatewayServer(GatewayConfig config, MessageBus bus) throws IOException {
        this.bus = java.util.Objects.requireNonNull(bus);
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress(config.port()), 256);
        // 功能：非守护监听线程使网关进程保持运行，直到收到关闭信号。
        acceptor = Thread.ofPlatform().name("gateway-acceptor").unstarted(this::acceptConnections);
    }

    /** 功能：启动入口；四个业务服务仍保持独立运行和独立部署。 */
    public void start() {
        acceptor.start();
    }

    /** 功能：返回实际监听端口，允许测试让操作系统选择空闲端口。 */
    public int port() {
        return listener.getLocalPort();
    }

    /** 功能：每条客户端连接交给一个虚拟线程；接收失败时短暂退避，避免描述符耗尽时忙循环。 */
    private void acceptConnections() {
        while (!closed.get()) {
            Socket socket;
            try {
                socket = listener.accept();
            } catch (IOException e) {
                if (closed.get()) return;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interrupted) {
                    return;
                }
                continue;
            }
            try {
                connections.execute(() -> serve(socket));
            } catch (RejectedExecutionException e) {
                closeQuietly(socket);
                return;
            }
        }
    }

    /** 功能：在同一条长连接上顺序处理请求；客户端断开、空闲超时或要求关闭时结束。 */
    private void serve(Socket socket) {
        clients.add(socket);
        try (socket) {
            if (closed.get()) return;
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(CLIENT_IDLE_MILLIS);
            InputStream in = new Http.Input(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            boolean keepAlive = true;
            while (keepAlive && !closed.get()) {
                Http.Request request;
                try {
                    request = readRequest(in, out);
                } catch (Http.MalformedMessage e) {
                    write(out, json(400, errorBody("INVALID_REQUEST", "Malformed request")), false, false);
                    return;
                } catch (Http.TooLarge e) {
                    write(out, json(413, errorBody("PAYLOAD_TOO_LARGE", "Request body too large")), false, false);
                    return;
                } catch (EOFException e) {
                    // A truncated client body is invalid; never publish a partial command.
                    write(out, json(400, errorBody("INVALID_REQUEST", "Incomplete request body")), false, false);
                    return;
                }
                if (request == null) return;
                keepAlive = request.keepAlive();
                write(out, forward(request), request.head(), keepAlive && !closed.get());
            }
        } catch (IOException e) {
            // 功能：客户端断开或空闲超时只结束本连接，不影响其他请求。
        } finally {
            clients.remove(socket);
        }
    }

    /** 功能：读取请求行、首部和完整正文；请求之间多余的空行按 HTTP/1.1 规则忽略。 */
    private static Http.Request readRequest(InputStream in, OutputStream out) throws IOException {
        String line;
        do {
            line = Http.readLine(in);
            if (line == null) return null;
        } while (line.isEmpty());
        String[] parts = line.split(" ", -1);
        if (parts.length != 3 || !Http.isToken(parts[0], 0, parts[0].length()) || !validTarget(parts[1])
                || !(parts[2].equals("HTTP/1.1") || parts[2].equals("HTTP/1.0"))) {
            throw new Http.MalformedMessage("Invalid request line");
        }
        List<Http.Header> headers = Http.readHeaders(in);
        if (parts[2].equals("HTTP/1.1") && "100-continue".equalsIgnoreCase(Http.header(headers, "Expect"))) {
            out.write(Http.latin1("HTTP/1.1 100 Continue\r\n\r\n"));
        }
        return new Http.Request(parts[0], parts[1], parts[2], headers, Http.readRequestBody(in, headers));
    }

    /** 功能：请求目标不能为空，也不能含空白或控制字符。 */
    private static boolean validTarget(String target) {
        if (target.isEmpty()) return false;
        for (int i = 0; i < target.length(); i++) {
            if (target.charAt(i) <= ' ' || target.charAt(i) == 0x7f) return false;
        }
        return true;
    }

    /** The HTTP contract remains synchronous: the service reply determines the HTTP status/body. */
    private Http.Response forward(Http.Request request) {
        URI uri;
        try {
            uri = new URI(request.target());
        } catch (URISyntaxException e) {
            return json(400, errorBody("INVALID_REQUEST", "Malformed request target"));
        }
        if (uri.isAbsolute() || uri.getRawAuthority() != null || uri.getRawFragment() != null) {
            return json(400, errorBody("INVALID_REQUEST", "Invalid request target"));
        }
        String path = uri.getRawPath();
        if (path == null || uri.getPath() == null || unsafePath(uri.getPath())) {
            return json(404, UNKNOWN_ROUTE);
        }
        String method = request.method();
        if ("/health".equals(path)) {
            return "GET".equals(method) ? json(200, "{\"status\":\"UP\",\"service\":\"gateway\"}")
                    : methodNotAllowed();
        }
        String body = new String(request.body(), StandardCharsets.UTF_8);
        try {
            if ("/items".equals(path)) {
                if (!"GET".equals(method)) return methodNotAllowed();
                //获取商品目录
                return relay(bus.request("catalog", "GET_CATALOG", uniqueKey(), "{}"));
            }
            if ("/transactions".equals(path)) {//Gateway 把 HTTP 请求映射为操作名，KafkaBus 发送消息，Checkout 收到消息后，再由 CheckoutMessages 按操作名分派处理。
                if (!"POST".equals(method)) return methodNotAllowed();
                //开始交易
                return relay(bus.request("checkout", "START_TRANSACTION", uniqueKey(), body));
            }
            if ("/inventory/low-stock".equals(path)) {
                if (!"GET".equals(method)) return methodNotAllowed();
                Integer threshold = queryInteger(uri, "threshold");
                //查询低库存
                return relay(bus.request("inventory", "GET_LOW_STOCK", uniqueKey(),
                        Json.write(new LowStockQuery(threshold))));
            }
            if ("/analytics/popular-items".equals(path)) {
                if (!"GET".equals(method)) return methodNotAllowed();
                Integer limit = queryInteger(uri, "limit");
                // The final load-client query must include all scans already acknowledged by Checkout.
                Reply checkpoint = bus.request("checkout", "SCAN_CHECKPOINT", uniqueKey(), "{}");
                if (checkpoint.status() != 200) return relay(checkpoint);
                Object count = Json.read(checkpoint.body(), Map.class).get("totalScans");
                if (!(count instanceof Number)) throw new IllegalStateException("Invalid scan checkpoint reply");
                //查询热门商品
                return relay(bus.request("analytics", "GET_POPULAR_ITEMS", uniqueKey(),
                        Json.write(new PopularItemsQuery(limit, ((Number) count).longValue()))));
            }
            String[] parts = path.split("/", -1);
            if ((parts.length == 3 || parts.length == 4) && "transactions".equals(parts[1])) {
                String transactionId = decodeSegment(parts[2]);
                if (transactionId == null) return json(404, UNKNOWN_ROUTE);
                if (parts.length == 3) {
                    if (!"GET".equals(method)) return methodNotAllowed();
                    //查询交易
                    return relay(bus.request("checkout", "GET_TRANSACTION", transactionId, "{}"));
                }
                if ("items".equals(parts[3])) {
                    if (!"POST".equals(method)) return methodNotAllowed();
                    //扫描商品GatewayServer maps the HTTP route to the SCAN_ITEM operation and uses the transaction ID as its business key
                    return relay(bus.request("checkout", "SCAN_ITEM", transactionId, body));//HTTP 请求怎样变成 Kafka 命令
                }
                if ("complete".equals(parts[3])) {
                    if (!"POST".equals(method)) return methodNotAllowed();
                    //完成交易
                    return relay(bus.request("checkout", "COMPLETE_TRANSACTION", transactionId, "{}"));
                }
            }
            return json(404, UNKNOWN_ROUTE);
        } catch (IllegalArgumentException e) {
            return json(400, errorBody("INVALID_REQUEST", "Invalid query parameter"));
        } catch (BusException e) {
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                if (cause instanceof TimeoutException) return json(504, errorBody("GATEWAY_TIMEOUT", "Message service timeout"));
            }
            return json(502, UPSTREAM_UNAVAILABLE);
        } catch (RuntimeException e) {
            return json(502, UPSTREAM_UNAVAILABLE);
        }
    }
    //将业务结果转换成 HTTP 响应converts that reply into an HTTP response
    private static Http.Response relay(Reply reply) {
        if (reply.status() < 200 || reply.status() > 599 || reply.body() == null) {
            throw new IllegalStateException("Invalid service reply");
        }
        return json(reply.status(), reply.body());
    }

    private static String errorBody(String code, String message) {
        return Json.write(Map.of("error", code, "message", message));
    }

    private static Http.Response methodNotAllowed() {
        return json(405, errorBody("METHOD_NOT_ALLOWED", "Method not allowed"));
    }

    private static String uniqueKey() {
        return UUID.randomUUID().toString();
    }

    private static Integer queryInteger(URI uri, String name) {
        Map<String, String> parameters = new HashMap<>();
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) return null;
        for (String pair : query.split("&", -1)) {
            String[] entry = pair.split("=", 2);
            String key = URLDecoder.decode(entry[0], StandardCharsets.UTF_8);
            String value = entry.length == 2 ? URLDecoder.decode(entry[1], StandardCharsets.UTF_8) : "";
            if (!name.equals(key) || parameters.putIfAbsent(key, value) != null || value.isEmpty()) {
                throw new IllegalArgumentException("Unexpected or repeated query parameter");
            }
        }
        return Integer.valueOf(parameters.get(name));
    }

    private static String decodeSegment(String raw) {
        // '+' is a literal path character; only form/query decoding maps it to a space.
        String decoded = URLDecoder.decode(raw.replace("+", "%2B"), StandardCharsets.UTF_8);
        if (decoded.isBlank() || decoded.indexOf('/') >= 0 || decoded.indexOf('\\') >= 0
                || decoded.indexOf(';') >= 0 || decoded.equals(".") || decoded.equals("..")) return null;
        for (int i = 0; i < decoded.length(); i++) {
            if (Character.isISOControl(decoded.charAt(i))) return null;
        }
        return decoded;
    }

    private static boolean unsafePath(String decodedPath) {
        if (decodedPath.indexOf('\\') >= 0) return true;
        for (String segment : decodedPath.split("/")) {
            String plain = segment.split(";", 2)[0];
            if (plain.equals(".") || plain.equals("..")) return true;
        }
        return false;
    }

    private record LowStockQuery(Integer threshold) {}
    private record PopularItemsQuery(Integer limit, long minimumScans) {}

    /** Preserves the service status/body and frames it for the existing synchronous HTTP client. */
    private static void write(OutputStream out, Http.Response response, boolean headRequest, boolean keepAlive)
            throws IOException {
        StringBuilder head = new StringBuilder(256);
        head.append("HTTP/1.1 ").append(response.status()).append(' ').append(response.reason()).append("\r\n");
        Set<String> excluded = Http.excludedHeaders(response.headers());
        excluded.add("content-length");
        for (Http.Header header : response.headers()) {
            if (!excluded.contains(header.name().toLowerCase(Locale.ROOT))) {
                head.append(header.name()).append(": ").append(header.value()).append("\r\n");
            }
        }
        byte[] body = response.body();
        boolean noBody = headRequest || response.status() == 204 || response.status() == 304;
        if (headRequest) {
            String length = Http.header(response.headers(), "Content-Length");
            if (length != null) head.append("Content-Length: ").append(length).append("\r\n");
        } else if (!noBody) {
            head.append("Content-Length: ").append(body.length).append("\r\n");
        }
        if (!keepAlive) head.append("Connection: close\r\n");
        byte[] bytes = Http.latin1(head.append("\r\n"));
        if (noBody || body.length == 0) {
            out.write(bytes);
            return;
        }
        // 功能：首部与正文一次写出，小响应只需要一次系统调用。
        byte[] message = Arrays.copyOf(bytes, bytes.length + body.length);
        System.arraycopy(body, 0, message, bytes.length, body.length);
        out.write(message);
    }

    /** 功能：生成网关自己的健康或错误 JSON；健康只表示入口进程可响应。 */
    private static Http.Response json(int status, String json) {
        String reason = switch (status) {
            case 200 -> "OK";
            case 201 -> "Created";
            case 204 -> "No Content";
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 409 -> "Conflict";
            case 413 -> "Content Too Large";
            case 500 -> "Internal Server Error";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            case 504 -> "Gateway Timeout";
            default -> "";
        };
        List<Http.Header> headers = List.of(new Http.Header("Content-Type", "application/json; charset=utf-8"),
                new Http.Header("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC))));
        return new Http.Response(status, reason, headers, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 功能：连接已不可用，关闭失败无需处理。
        }
    }

    /** 功能：停止接收、断开客户端连接并释放虚拟线程；消息总线由启动入口关闭；重复关闭不会重复清理。 */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                listener.close();
            } catch (IOException ignored) {
                // 功能：监听套接字关闭失败时继续释放其他资源。
            }
            clients.forEach(GatewayServer::closeQuietly);
            // 功能：中断仍在等待上游响应的虚拟线程，其套接字随之关闭。
            connections.shutdownNow();
            try {
                connections.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
