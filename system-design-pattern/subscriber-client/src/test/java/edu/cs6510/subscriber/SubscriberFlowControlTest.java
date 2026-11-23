package edu.cs6510.subscriber;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.net.http.WebSocket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class SubscriberFlowControlTest {
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean aborted = new AtomicBoolean();
    private final WebSocket socket = (WebSocket) Proxy.newProxyInstance(
            WebSocket.class.getClassLoader(), new Class<?>[]{WebSocket.class}, (proxy, method, args) -> {
                if (method.getName().equals("request")) demand.addAndGet((long) args[0]);
                if (method.getName().equals("abort")) aborted.set(true);
                return null;
            });

    @Test void completeMessageDoesNotRequestMoreBeforeWorkerProcessesIt() {
        var listener = new SubscriberMain.Listener();
        listener.onOpen(socket);
        listener.onText(socket, "message", true);
        assertEquals(4, demand.get());
        assertEquals("message", listener.messages.poll());
        assertFalse(aborted.get());
    }
    @Test void fragmentsContinueUntilOneCompleteMessage() {
        var listener = new SubscriberMain.Listener();
        listener.onOpen(socket);
        listener.onText(socket, "first", false);
        assertEquals(5, demand.get());
        listener.onText(socket, "last", true);
        assertEquals(5, demand.get());
        assertEquals("firstlast", listener.messages.poll());
    }
    @Test void largeBurstWithWorkerDrivenDemandDoesNotOverflow() {
        var listener = new SubscriberMain.Listener();
        listener.onOpen(socket);
        for (int index = 0; index < 10000; index++) {
            listener.onText(socket, "message-" + index, true);
            assertEquals("message-" + index, listener.messages.poll());
            socket.request(1);
        }
        assertFalse(aborted.get());
        assertEquals(10004, demand.get());
    }
    @Test void onlyTransientProtocolErrorsAreRetryable() {
        assertTrue(SubscriberMain.retryableError("SUBSCRIBER_BUSY"));
        assertTrue(SubscriberMain.retryableError("CHANNEL_BACKPRESSURE"));
        assertTrue(SubscriberMain.retryableError("TEMPORARY_FAILURE"));
        assertFalse(SubscriberMain.retryableError("INVALID_REQUEST"));
        assertFalse(SubscriberMain.retryableError(null));
    }
    @Test void onePermanentlyFailedThreadFailsTheClient() {
        var failures = new ConcurrentLinkedQueue<Throwable>();
        SubscriberMain.checkFailures(failures);
        failures.add(new IllegalArgumentException("bad credentials"));
        assertThrows(IllegalStateException.class, () -> SubscriberMain.checkFailures(failures));
    }
}
