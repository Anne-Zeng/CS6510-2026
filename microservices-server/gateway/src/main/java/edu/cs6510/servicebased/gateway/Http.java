package edu.cs6510.servicebased.gateway;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** 功能：网关两侧共用的 HTTP/1.1 报文读写；只实现代理所需语法，拒绝有歧义的正文边界。 */
final class Http {
    // 功能：逐跳首部只对一段连接有效，转发时必须重新生成，不能从一侧带到另一侧。
    static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade");
    // 功能：限制首行、首部和正文大小，避免恶意或损坏的报文耗尽网关内存。
    static final int MAX_LINE = 16 * 1024;
    static final int MAX_HEADERS = 100;
    static final int MAX_BODY = 16 * 1024 * 1024;
    private static final byte[] EMPTY = new byte[0];

    /** 功能：保持原始大小写和顺序的首部；按 ISO-8859-1 读写，字节可原样往返。 */
    record Header(String name, String value) {}

    /** 功能：已完整读取的请求；target 保留客户端发送的原始编码。 */
    record Request(String method, String target, String version, List<Header> headers, byte[] body) {
        boolean head() {
            return "HEAD".equals(method);
        }

        /** 功能：HTTP/1.1 默认长连接，HTTP/1.0 或 Connection: close 时响应后关闭。 */
        boolean keepAlive() {
            return "HTTP/1.1".equals(version) && !connectionTokens(headers).contains("close");
        }
    }

    /** 功能：已完整读取的响应；reusable 表示上游连接的报文边界明确且可继续复用。 */
    record Response(int status, String reason, List<Header> headers, byte[] body, boolean reusable) {}

    /** 功能：请求报文本身不合法时使用，网关返回 400 后关闭连接。 */
    static final class MalformedMessage extends IOException {
        MalformedMessage(String message) {
            super(message);
        }
    }

    /** 功能：正文超过网关上限时使用，返回 413 后关闭连接。 */
    static final class TooLarge extends IOException {
        TooLarge() {
            super("Message body too large");
        }
    }

    private Http() {
    }

    /** 功能：读取一个以 CRLF（兼容单独 LF）结束的行；在第一个字节前遇到 EOF 时返回 null。 */
    static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder(64);
        int b;
        while ((b = in.read()) != '\n') {
            if (b == -1) {
                if (line.isEmpty()) return null;
                throw new EOFException("Connection closed inside a header line");
            }
            if (line.length() == MAX_LINE) throw new MalformedMessage("Header line too long");
            line.append((char) b);
        }
        int end = line.length();
        if (end > 0 && line.charAt(end - 1) == '\r') line.setLength(end - 1);
        return line.toString();
    }

    /** 功能：读取空行之前的全部首部；拒绝折叠行、非法名称、控制字符和过多首部，避免向上游注入报文。 */
    static List<Header> readHeaders(InputStream in) throws IOException {
        List<Header> headers = new ArrayList<>(16);
        while (true) {
            String line = readLine(in);
            if (line == null) throw new EOFException("Connection closed inside headers");
            if (line.isEmpty()) return headers;
            int colon = line.indexOf(':');
            if (colon <= 0 || !isToken(line, 0, colon)) throw new MalformedMessage("Invalid header name");
            String value = line.substring(colon + 1).strip();
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if ((c < 0x20 && c != '\t') || c == 0x7f) throw new MalformedMessage("Invalid header value");
            }
            if (headers.size() == MAX_HEADERS) throw new MalformedMessage("Too many headers");
            headers.add(new Header(line.substring(0, colon), value));
        }
    }

    /** 功能：判断是否为 RFC 9110 token，用于方法名和首部名称。 */
    static boolean isToken(String text, int from, int to) {
        if (from >= to) return false;
        for (int i = from; i < to; i++) {
            char c = text.charAt(i);
            boolean letterOrDigit = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
            if (!letterOrDigit && "!#$%&'*+-.^_`|~".indexOf(c) < 0) return false;
        }
        return true;
    }

    /** 功能：按名称忽略大小写读取首个首部值。 */
    static String header(List<Header> headers, String name) {
        for (Header header : headers) {
            if (header.name().equalsIgnoreCase(name)) return header.value();
        }
        return null;
    }

    /** 功能：Connection 首部列出的选项（小写），用于识别 close 和自定义逐跳首部。 */
    static Set<String> connectionTokens(List<Header> headers) {
        Set<String> tokens = new HashSet<>();
        for (Header header : headers) {
            if (header.name().equalsIgnoreCase("connection")) {
                for (String token : header.value().split(",")) {
                    tokens.add(token.trim().toLowerCase(Locale.ROOT));
                }
            }
        }
        return tokens;
    }

    /** 功能：转发时需排除的首部：标准逐跳首部及 Connection 声明的自定义逐跳首部。 */
    static Set<String> excludedHeaders(List<Header> headers) {
        Set<String> excluded = new HashSet<>(HOP_BY_HOP);
        excluded.addAll(connectionTokens(headers));
        return excluded;
    }

    /** 功能：按 Content-Length 或 chunked 读取请求正文；两者同时出现或长度非法时拒绝，防止报文走私。 */
    static byte[] readRequestBody(InputStream in, List<Header> headers) throws IOException {
        String encoding = header(headers, "Transfer-Encoding");
        String length = header(headers, "Content-Length");
        if (encoding != null) {
            if (length != null || !isChunked(encoding)) throw new MalformedMessage("Unsupported request framing");
            return readChunked(in);
        }
        return length == null ? EMPTY : readFixed(in, contentLength(headers));
    }

    /** 功能：按 HTTP 规则读取响应正文；无法确定长度时读到连接关闭，并标记连接不可复用。 */
    static Response readResponseBody(InputStream in, int status, String reason, List<Header> headers,
                                     boolean headRequest, boolean persistent) throws IOException {
        if (headRequest || status == 204 || status == 304) {
            return new Response(status, reason, headers, EMPTY, persistent);
        }
        String encoding = header(headers, "Transfer-Encoding");
        if (encoding != null) {
            if (isChunked(encoding)) return new Response(status, reason, headers, readChunked(in), persistent);
            return new Response(status, reason, headers, readToEnd(in), false);
        }
        if (header(headers, "Content-Length") != null) {
            return new Response(status, reason, headers, readFixed(in, contentLength(headers)), persistent);
        }
        return new Response(status, reason, headers, readToEnd(in), false);
    }

    /** 功能：多个 Content-Length 必须一致且为非负十进制数。 */
    static long contentLength(List<Header> headers) throws IOException {
        long length = -1;
        for (Header header : headers) {
            if (!header.name().equalsIgnoreCase("content-length")) continue;
            for (String value : header.value().split(",")) {
                long parsed = parseLength(value.trim());
                if (length >= 0 && parsed != length) throw new MalformedMessage("Conflicting Content-Length");
                length = parsed;
            }
        }
        return length;
    }

    private static long parseLength(String value) throws IOException {
        if (value.isEmpty() || value.length() > 18) throw new MalformedMessage("Invalid Content-Length");
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < '0' || value.charAt(i) > '9') throw new MalformedMessage("Invalid Content-Length");
        }
        return Long.parseLong(value);
    }

    private static boolean isChunked(String encoding) {
        String[] codings = encoding.split(",");
        return codings[codings.length - 1].trim().equalsIgnoreCase("chunked");
    }

    /** 功能：精确读取声明长度；提前结束视为不完整报文。 */
    private static byte[] readFixed(InputStream in, long length) throws IOException {
        if (length > MAX_BODY) throw new TooLarge();
        if (length == 0) return EMPTY;
        byte[] body = in.readNBytes((int) length);
        if (body.length != length) throw new EOFException("Incomplete message body");
        return body;
    }

    /** 功能：解码 chunked 正文并丢弃 trailer；转发时改用精确的 Content-Length。 */
    private static byte[] readChunked(InputStream in) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        while (true) {
            String line = readLine(in);
            if (line == null) throw new EOFException("Connection closed inside chunked body");
            int extension = line.indexOf(';');
            String hex = (extension >= 0 ? line.substring(0, extension) : line).trim();
            if (hex.isEmpty() || hex.length() > 8) throw new MalformedMessage("Invalid chunk size");
            int size;
            try {
                size = Integer.parseInt(hex, 16);
            } catch (NumberFormatException e) {
                throw new MalformedMessage("Invalid chunk size");
            }
            if (size < 0) throw new MalformedMessage("Invalid chunk size");
            if (size == 0) {
                String trailer;
                do {
                    trailer = readLine(in);
                    if (trailer == null) throw new EOFException("Connection closed inside trailers");
                } while (!trailer.isEmpty());
                return body.toByteArray();
            }
            if (body.size() + (long) size > MAX_BODY) throw new TooLarge();
            byte[] chunk = in.readNBytes(size);
            if (chunk.length != size) throw new EOFException("Incomplete chunk");
            body.write(chunk);
            String end = readLine(in);
            if (end == null || !end.isEmpty()) throw new MalformedMessage("Missing chunk terminator");
        }
    }

    /** 功能：读取由关闭连接界定的正文。 */
    private static byte[] readToEnd(InputStream in) throws IOException {
        byte[] body = in.readNBytes(MAX_BODY + 1);
        if (body.length > MAX_BODY) throw new TooLarge();
        return body;
    }

    /** 功能：首部按 ISO-8859-1 编码，与读取方式对应，保证非 ASCII 字节原样转发。 */
    static byte[] latin1(CharSequence text) {
        return text.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    /** 功能：只由一个虚拟线程使用的输入缓冲；逐字节解析首部时不需要 BufferedInputStream 的锁。 */
    static final class Input extends InputStream {
        private final InputStream source;
        private final byte[] buffer = new byte[16 * 1024];
        private int position;
        private int limit;

        Input(InputStream source) {
            this.source = source;
        }

        @Override
        public int read() throws IOException {
            if (position == limit && !fill()) return -1;
            return buffer[position++] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            if (length == 0) return 0;
            if (position == limit) {
                if (length >= buffer.length) return source.read(target, offset, length);
                if (!fill()) return -1;
            }
            int count = Math.min(length, limit - position);
            System.arraycopy(buffer, position, target, offset, count);
            position += count;
            return count;
        }

        /** 功能：缓冲中未读字节加上套接字中已到达的字节。 */
        @Override
        public int available() throws IOException {
            return limit - position + source.available();
        }

        private boolean fill() throws IOException {
            int count = source.read(buffer, 0, buffer.length);
            if (count <= 0) return false;
            position = 0;
            limit = count;
            return true;
        }
    }
}
