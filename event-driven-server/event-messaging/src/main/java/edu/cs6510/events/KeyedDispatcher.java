package edu.cs6510.events;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** Serialize handlers for a business key while allowing unrelated keys to make progress.
 * Failed deliveries are retried by the consumer and do not permanently block the key. */
final class KeyedDispatcher {
    private final Executor executor;
    private final Map<String, CompletableFuture<Void>> tails = new HashMap<>();

    KeyedDispatcher(Executor executor) { this.executor = executor; }

    synchronized CompletableFuture<Void> submit(String key, Supplier<CompletionStage<Void>> action) {
        var previous = tails.getOrDefault(key, CompletableFuture.completedFuture(null));
        var next = previous.handle((ignored, failure) -> null).thenComposeAsync(ignored -> action.get(), executor);
        tails.put(key, next);
        next.whenComplete((ignored, failure) -> {
            synchronized (this) { tails.remove(key, next); }
        });
        return next;
    }
}
