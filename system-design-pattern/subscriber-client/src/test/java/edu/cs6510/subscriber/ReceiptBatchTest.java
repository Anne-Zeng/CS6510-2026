package edu.cs6510.subscriber;

import edu.cs6510.events.Json;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;

class ReceiptBatchTest {
    private WebSocket socket(java.util.function.Consumer<String> sent) {
        return (WebSocket) Proxy.newProxyInstance(WebSocket.class.getClassLoader(), new Class<?>[]{WebSocket.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("sendText")) {
                        sent.accept(arguments[0].toString());
                        return CompletableFuture.completedFuture(proxy);
                    }
                    return null;
                });
    }

    @Test void flushesBeforeOneBatchAckAndDeduplicatesLog() throws Exception {
        var operations = new ArrayList<String>();
        var log = new StringWriter() {
            @Override public void flush() { operations.add("flush"); }
        };
        var seen = new HashSet<UUID>();
        var batch = new ReceiptBatch(log, seen);
        var identifier = UUID.randomUUID();
        batch.record(Map.of("notificationId", identifier.toString()));
        batch.record(Map.of("notificationId", identifier.toString()));
        assertTrue(seen.isEmpty());
        assertEquals(1, log.toString().lines().count());
        batch.flush(socket(body -> {
            assertTrue(seen.contains(identifier));
            assertEquals(List.of(identifier.toString(), identifier.toString()), Json.read(body, Map.class).get("notificationIds"));
            operations.add("ack");
        }));
        assertEquals(List.of("flush", "ack"), operations);
        assertTrue(batch.isEmpty());
    }

    @Test void failedFlushNeverAcknowledgesOrMarksSeen() throws Exception {
        var seen = new HashSet<UUID>();
        var writer = new java.io.Writer() {
            @Override public void write(char[] data, int offset, int length) {}
            @Override public void flush() throws IOException { throw new IOException("disk failure"); }
            @Override public void close() {}
        };
        var batch = new ReceiptBatch(writer, seen);
        batch.record(Map.of("notificationId", UUID.randomUUID().toString()));
        assertThrows(IOException.class, () -> batch.flush(socket(body -> fail("ACK before successful flush"))));
        assertTrue(seen.isEmpty());
    }

    @Test void reconnectAfterLostAckDoesNotDuplicateLog() throws Exception {
        var seen = new HashSet<UUID>();
        var log = new StringWriter();
        var message = Map.of("notificationId", UUID.randomUUID().toString());
        var first = new ReceiptBatch(log, seen);
        first.record(message);
        assertThrows(IllegalStateException.class, () -> first.flush(socket(body -> { throw new IllegalStateException("disconnect"); })));
        var second = new ReceiptBatch(log, seen);
        second.record(message);
        second.flush(socket(body -> {}));
        assertEquals(1, log.toString().lines().count());
        assertTrue(second.isEmpty());
    }

    @Test void smallBatchHasTimeBoundAndFullBatchFlushesImmediately() throws Exception {
        var batch = new ReceiptBatch(new StringWriter(), new HashSet<>());
        assertFalse(batch.due());
        batch.record(Map.of("notificationId", UUID.randomUUID().toString()));
        Thread.sleep(30);
        assertTrue(batch.due());
        batch.flush(socket(body -> {}));
        for (int count = 0; count < 32; count++) batch.record(Map.of("notificationId", UUID.randomUUID().toString()));
        assertTrue(batch.due());
    }
}
