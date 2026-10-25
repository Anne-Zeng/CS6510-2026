package edu.cs6510.servicebased.gateway;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/** 功能：一个业务服务的 HTTP/1.1 长连接池；每条连接一次只承载一个请求，读完完整响应后归还。 */
final class Upstream implements Closeable {
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    // 功能：与原网关一致，上游 60 秒没有响应时返回 504。
    private static final int READ_TIMEOUT_MILLIS = 60_000;
    // 功能：空闲连接远早于服务端 keep-alive 超时淘汰，避免复用对端即将关闭的连接。
    private static final long MAX_IDLE_NANOS = TimeUnit.SECONDS.toNanos(15);
    private static final Set<String> IDEMPOTENT = Set.of("GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE");
    private final InetSocketAddress address;
    private final String host;
    private final Deque<Connection> idle = new ConcurrentLinkedDeque<>();
    private volatile boolean closed;

    /** 功能：本地业务服务之间使用明文 HTTP；配置为 HTTPS 时启动即报错，而不是在请求时失败。 */
    Upstream(URI root) {
        if (!"http".equalsIgnoreCase(root.getScheme())) {
            throw new IllegalArgumentException("Gateway forwards plain HTTP only: " + root);
        }
        int port = root.getPort() >= 0 ? root.getPort() : 80;
        address = new InetSocketAddress(root.getHost(), port);
        host = root.getPort() >= 0 ? root.getHost() + ":" + port : root.getHost();
    }

    /** 功能：发送一个请求并读取完整响应；复用连接在收到任何响应字节前失败时，只为幂等方法重试一次。 */
    Http.Response exchange(String method, String target, List<Http.Header> headers, byte[] body) throws IOException {
        byte[] head = requestHead(method, target, headers, body.length);
        boolean headRequest = "HEAD".equals(method);
        for (int attempt = 0; ; attempt++) {
            Connection connection = borrow();
            try {
                connection.write(head, body);
                Http.Response response = connection.read(headRequest);
                if (response.reusable() && !closed) {
                    connection.idleSince = System.nanoTime();
                    idle.offerFirst(connection);
                } else {
                    connection.close();
                }
                return response;
            } catch (IOException | RuntimeException e) {
                connection.close();
                if (attempt == 0 && connection.reused && !connection.responseStarted && IDEMPOTENT.contains(method)) continue;
                throw e;
            }
        }
    }

    /** 功能：请求行与首部一次编码；Host 和 Content-Length 由网关按本段连接重新生成。 */
    private byte[] requestHead(String method, String target, List<Http.Header> headers, int length) {
        StringBuilder text = new StringBuilder(256);
        text.append(method).append(' ').append(target).append(" HTTP/1.1\r\nHost: ").append(host).append("\r\n");
        for (Http.Header header : headers) {
            text.append(header.name()).append(": ").append(header.value()).append("\r\n");
        }
        if (length > 0 || !(method.equals("GET") || method.equals("HEAD"))) {
            text.append("Content-Length: ").append(length).append("\r\n");
        }
        return Http.latin1(text.append("\r\n"));
    }

    /** 功能：优先复用最近归还的连接；超时或已被对端关闭的连接直接丢弃。 */
    private Connection borrow() throws IOException {
        Connection connection;
        while ((connection = idle.pollFirst()) != null) {
            if (connection.usable()) {
                connection.reused = true;
                connection.responseStarted = false;
                return connection;
            }
            connection.close();
        }
        return Connection.open(address);
    }

    /** 功能：网关关闭时释放全部空闲连接；正在使用的连接在请求结束后关闭。 */
    @Override
    public void close() {
        closed = true;
        Connection connection;
        while ((connection = idle.pollFirst()) != null) connection.close();
    }

    /** 功能：单条阻塞式上游连接；读超时使用 SO_TIMEOUT，空闲检查使用一次非阻塞读取。 */
    private static final class Connection implements Closeable {
        private final SocketChannel channel;
        private final InputStream in;
        private final OutputStream out;
        private final ByteBuffer probe = ByteBuffer.allocate(1);
        private long idleSince;
        private boolean reused;
        private boolean responseStarted;

        private Connection(SocketChannel channel) throws IOException {
            this.channel = channel;
            in = new Http.Input(channel.socket().getInputStream());
            out = channel.socket().getOutputStream();
        }

        static Connection open(InetSocketAddress address) throws IOException {
            SocketChannel channel = SocketChannel.open();
            try {
                channel.socket().connect(address, CONNECT_TIMEOUT_MILLIS);
                channel.socket().setTcpNoDelay(true);
                channel.socket().setSoTimeout(READ_TIMEOUT_MILLIS);
                return new Connection(channel);
            } catch (IOException | RuntimeException e) {
                channel.close();
                throw e;
            }
        }

        /** 功能：空闲期间对端若已关闭或发来多余字节，该连接就不能再承载新请求。 */
        boolean usable() {
            if (System.nanoTime() - idleSince > MAX_IDLE_NANOS) return false;
            try {
                if (in.available() > 0) return false;
                channel.configureBlocking(false);
                try {
                    probe.clear();
                    return channel.read(probe) == 0;
                } finally {
                    channel.configureBlocking(true);
                }
            } catch (IOException e) {
                return false;
            }
        }

        void write(byte[] head, byte[] body) throws IOException {
            if (body.length == 0) {
                out.write(head);
            } else {
                byte[] message = new byte[head.length + body.length];
                System.arraycopy(head, 0, message, 0, head.length);
                System.arraycopy(body, 0, message, head.length, body.length);
                out.write(message);
            }
        }

        /** 功能：跳过 1xx 临时响应，读取最终响应；只有 HTTP/1.1 且未要求关闭时才继续复用。 */
        Http.Response read(boolean headRequest) throws IOException {
            while (true) {
                String status = Http.readLine(in);
                if (status == null) throw new EOFException("Upstream closed the connection");
                responseStarted = true;
                if (!status.startsWith("HTTP/1.") || status.length() < 12 || status.charAt(8) != ' ') {
                    throw new IOException("Invalid upstream status line");
                }
                int code;
                try {
                    code = Integer.parseInt(status.substring(9, 12));
                } catch (NumberFormatException e) {
                    throw new IOException("Invalid upstream status code");
                }
                String reason = status.length() > 13 ? status.substring(13) : "";
                List<Http.Header> headers = Http.readHeaders(in);
                if (code == 101) throw new IOException("Unexpected upstream protocol switch");
                if (code < 200) continue;
                boolean persistent = status.startsWith("HTTP/1.1")
                        && !Http.connectionTokens(headers).contains("close");
                return Http.readResponseBody(in, code, reason, headers, headRequest, persistent);
            }
        }

        @Override
        public void close() {
            try {
                channel.close();
            } catch (IOException ignored) {
                // 功能：关闭失败不影响已经返回给客户端的结果。
            }
        }
    }
}
