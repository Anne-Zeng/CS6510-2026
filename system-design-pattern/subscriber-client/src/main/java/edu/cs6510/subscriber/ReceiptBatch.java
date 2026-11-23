package edu.cs6510.subscriber;

import edu.cs6510.events.Json;
import java.io.IOException;
import java.io.Writer;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class ReceiptBatch {
    static final int LIMIT = 32;
    private final Writer log;
    private final Set<UUID> seen;
    private final Set<UUID> buffered = new HashSet<>();
    private final List<String> acknowledgements = new ArrayList<>();
    private long started;

    ReceiptBatch(Writer log, Set<UUID> seen) {
        this.log = log;
        this.seen = seen;
    }
//检查 notification ID，避免重复记录，添加 receivedAt 接收时间，将通知写成一行 JSON，随后 flush(...) 先刷新日志缓冲，再发送 ACK
    void record(Map<?, ?> message) throws IOException {
        String identifier = (String) message.get("notificationId");
        UUID parsed = UUID.fromString(identifier);
        if (acknowledgements.isEmpty()) started = System.nanoTime();
        if (!seen.contains(parsed) && buffered.add(parsed)) {
            var entry = new LinkedHashMap<String, Object>();
            message.forEach((key, value) -> entry.put((String) key, value));
            entry.put("receivedAt", Instant.now());
            log.write(Json.write(entry));
            log.write('\n');
        }
        acknowledgements.add(identifier);
    }

    boolean isEmpty() { return acknowledgements.isEmpty(); }

    boolean due() {
        return acknowledgements.size() >= LIMIT
                || (!isEmpty() && System.nanoTime() - started >= 20_000_000L);
    }

    void persist() throws IOException {
        log.flush();
        seen.addAll(buffered);
        buffered.clear();
    }

    void flush(WebSocket socket) throws IOException {
        if (isEmpty()) return;
        persist();
        socket.sendText(Json.write(Map.of("type", "ACKS", "notificationIds", List.copyOf(acknowledgements))), true).join();
        acknowledgements.clear();
    }
}
