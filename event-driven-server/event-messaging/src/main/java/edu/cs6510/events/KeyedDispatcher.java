package edu.cs6510.events;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
//根据 key 调度任务，正常处理时同 key 顺序执行，不同 key 可以并发
/** Serialize handlers for a business key while allowing unrelated keys to make progress.
 * Failed deliveries are retried by the consumer and do not permanently block the key. */
final class KeyedDispatcher {
    private final Executor executor;
    //内部为每个 key 保存一个任务链,新任务接到同一个 key 的前一个任务后面。
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
