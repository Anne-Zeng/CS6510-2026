import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects per-operation latency samples and error counts across every
 * simulated station thread, and computes summary statistics at the end of
 * a run. Kept deliberately simple (sort-and-index percentiles) since a
 * single test run at classroom scale produces at most a few hundred
 * thousand samples per operation - sorting that is essentially instant and
 * avoids pulling in a histogram library.
 */
public final class Metrics {

    public enum Operation { START_TRANSACTION, SCAN_ITEM, COMPLETE_TRANSACTION }

    public record Summary(
            String operation,
            long successCount,
            long errorCount,
            double meanMs,
            double p50Ms,
            double p95Ms,
            double p99Ms,
            double minMs,
            double maxMs
    ) {
        double errorRate() {
            long total = successCount + errorCount;
            return total == 0 ? 0.0 : (double) errorCount / total;
        }
    }

    private final Map<Operation, ConcurrentLinkedQueue<Long>> latenciesNanos = new EnumMap<>(Operation.class);
    private final Map<Operation, AtomicLong> errorCounts = new EnumMap<>(Operation.class);

    public Metrics() {
        for (Operation op : Operation.values()) {
            latenciesNanos.put(op, new ConcurrentLinkedQueue<>());
            errorCounts.put(op, new AtomicLong());
        }
    }

    public void recordSuccess(Operation op, long elapsedNanos) {
        latenciesNanos.get(op).add(elapsedNanos);
    }

    public void recordError(Operation op) {
        errorCounts.get(op).incrementAndGet();
    }

    public Summary summarize(Operation op) {
        Long[] boxed = latenciesNanos.get(op).toArray(new Long[0]);
        long[] nanos = new long[boxed.length];
        for (int i = 0; i < boxed.length; i++) nanos[i] = boxed[i];
        Arrays.sort(nanos);

        long errors = errorCounts.get(op).get();
        if (nanos.length == 0) {
            return new Summary(op.name(), 0, errors, 0, 0, 0, 0, 0, 0);
        }

        double mean = Arrays.stream(nanos).average().orElse(0) / 1_000_000.0;
        double p50 = percentile(nanos, 0.50) / 1_000_000.0;
        double p95 = percentile(nanos, 0.95) / 1_000_000.0;
        double p99 = percentile(nanos, 0.99) / 1_000_000.0;
        double min = nanos[0] / 1_000_000.0;
        double max = nanos[nanos.length - 1] / 1_000_000.0;

        return new Summary(op.name(), nanos.length, errors, mean, p50, p95, p99, min, max);
    }

    private static double percentile(long[] sortedNanos, double p) {
        int idx = (int) Math.ceil(p * sortedNanos.length) - 1;
        idx = Math.max(0, Math.min(sortedNanos.length - 1, idx));
        return sortedNanos[idx];
    }

    public long totalSuccesses(Operation op) {
        return latenciesNanos.get(op).size();
    }
}
