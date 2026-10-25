package edu.cs6510.servicebased.gateway;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 功能：提供单一 HTTP 入口，仅按路径转发请求；不承载业务规则或服务间调用。
 * 每条客户端连接由一个虚拟线程顺序处理：读请求、经上游长连接转发、写回响应，中间没有线程切换。 */
public final class GatewayServer implements AutoCloseable {
    // 功能：这些首部由网关按上游连接重新生成，或只属于客户端到网关这一段。
    private static final Set<String> CLIENT_MANAGED = Set.of("host", "content-length", "expect", "http2-settings");
    // 功能：客户端连接空闲 30 秒后关闭，与原 JDK HttpServer 的默认空闲时间一致。
    private static final int CLIENT_IDLE_MILLIS = 30_000;
    private static final String UNKNOWN_ROUTE = "{\"error\":\"Unknown route\"}";
    private static final String UPSTREAM_UNAVAILABLE = "{\"error\":\"Upstream unavailable\"}";
    private final Upstream catalog;
    private final Upstream checkout;
    private final Upstream analytics;
    private final Upstream inventory;
    private final ServerSocket listener;
    private final Thread acceptor;
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 功能：绑定监听端口并准备四个上游连接池，不立即开始接收请求。 */
    public GatewayServer(GatewayConfig config) throws IOException {
        catalog = new Upstream(config.catalogUrl());
        checkout = new Upstream(config.checkoutUrl());
        analytics = new Upstream(config.analyticsUrl());
        inventory = new Upstream(config.inventoryUrl());
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
                    write(out, json(400, "{\"error\":\"Malformed request\"}"), false, false);
                    return;
                } catch (Http.TooLarge e) {
                    write(out, json(413, "{\"error\":\"Request body too large\"}"), false, false);
                    return;
                } catch (EOFException e) {
                    // 功能：声明长度后提前结束上传，不能截断后转发；与原网关一样返回 502。
                    write(out, json(502, UPSTREAM_UNAVAILABLE), false, false);
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

    /** 功能：处理健康检查、路由校验和转发；连接失败返回 502，超时返回 504。 */
    private Http.Response forward(Http.Request request) {
        URI uri;
        try {
            uri = new URI(request.target());
        } catch (URISyntaxException e) {
            return json(400, "{\"error\":\"Malformed request target\"}");
        }
        String path = uri.getRawPath();
        if ("/health".equals(path)) return json(200, "{\"status\":\"UP\",\"service\":\"gateway\"}");
        // 功能：拒绝路径穿越，避免后端规范化路径后绕过 /internal 路由边界。
        Upstream backend = path == null || uri.getPath() == null || unsafePath(uri.getPath()) ? null : backendFor(path);
        if (backend == null) return json(404, UNKNOWN_ROUTE);
        // 功能：字符串拼接保留客户端的原始编码路径和查询参数。
        String target = uri.getRawQuery() == null ? path : path + "?" + uri.getRawQuery();
        Set<String> excluded = Http.excludedHeaders(request.headers());
        excluded.addAll(CLIENT_MANAGED);
        List<Http.Header> headers = new ArrayList<>(request.headers().size());
        for (Http.Header header : request.headers()) {
            if (!excluded.contains(header.name().toLowerCase(Locale.ROOT))) headers.add(header);
        }
        try {
            return backend.exchange(request.method(), target, headers, request.body());
        } catch (SocketTimeoutException e) {
            return json(504, "{\"error\":\"Upstream timeout\"}");
        } catch (IOException | RuntimeException e) {
            return json(502, UPSTREAM_UNAVAILABLE);
        }
    }

    /** 功能：按完整路径段匹配业务边界，防止 /items-other 等名称误入目录服务。 */
    private Upstream backendFor(String path) {
        if (matches(path, "/items")) return catalog;
        if (matches(path, "/transactions")) return checkout;
        if (matches(path, "/inventory")) return inventory;
        return matches(path, "/analytics") ? analytics : null;
    }

    private static boolean unsafePath(String decodedPath) {
        if (decodedPath.indexOf('\\') >= 0) return true;
        for (String segment : decodedPath.split("/")) {
            String plain = segment.split(";", 2)[0];
            if (plain.equals(".") || plain.equals("..")) return true;
        }
        return false;
    }

    /** 功能：允许资源本身及其子路径，不进行含糊的字符串前缀路由。 */
    private static boolean matches(String path, String prefix) {
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    /** 功能：返回上游真实状态、端到端首部和正文；按实际正文长度重新计算 framing，保持客户端长连接可复用。 */
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
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 413 -> "Content Too Large";
            case 502 -> "Bad Gateway";
            case 504 -> "Gateway Timeout";
            default -> "";
        };
        List<Http.Header> headers = List.of(new Http.Header("Content-Type", "application/json; charset=utf-8"),
                new Http.Header("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC))));
        return new Http.Response(status, reason, headers, json.getBytes(StandardCharsets.UTF_8), true);
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 功能：连接已不可用，关闭失败无需处理。
        }
    }

    /** 功能：停止接收、断开客户端连接并释放上游连接池与虚拟线程；重复关闭不会重复清理。 */
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
            catalog.close();
            checkout.close();
            analytics.close();
            inventory.close();
        }
    }
}
