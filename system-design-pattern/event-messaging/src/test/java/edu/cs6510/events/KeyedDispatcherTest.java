package edu.cs6510.events;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class KeyedDispatcherTest {
    @Test void anotherStationProceedsWhileOneCommandAwaitsItsDurableReply() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var dispatcher = new KeyedDispatcher(executor);
            var firstReplyAcknowledged = new CompletableFuture<Void>();
            var firstStarted = new CountDownLatch(1);
            var secondStarted = new AtomicBoolean();
            var first = dispatcher.submit("station-A", () -> {
                firstStarted.countDown();
                return firstReplyAcknowledged;
            });
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            var second = dispatcher.submit("station-A", () -> {
                secondStarted.set(true);
                return CompletableFuture.completedFuture(null);
            });
            var independent = dispatcher.submit("station-B", () -> CompletableFuture.completedFuture(null));
            independent.get(2, TimeUnit.SECONDS);
            assertFalse(first.isDone());
            assertFalse(secondStarted.get());
            firstReplyAcknowledged.complete(null);
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);
            assertTrue(secondStarted.get());
        }
    }

    @Test void aFailedCommandDoesNotPermanentlyPoisonTheBusinessKey() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var dispatcher = new KeyedDispatcher(executor);
            var failed = dispatcher.submit("transaction-A", () -> {
                throw new IllegalStateException("Database temporarily unavailable");
            });
            assertThrows(java.util.concurrent.ExecutionException.class, () -> failed.get(2, TimeUnit.SECONDS));
            dispatcher.submit("transaction-A", () -> CompletableFuture.completedFuture(null)).get(2, TimeUnit.SECONDS);
        }
    }
}
