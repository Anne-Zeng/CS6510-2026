package edu.cs6510.layered_server.analytics;

import edu.cs6510.layered_server.persistence.ScanEventStore;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.stream.Collectors;

/** Exact count windows over immutable, durably ordered scan events. */
@Component
public class PersistentScanWindow implements ScanWindow {
    private static final int WINDOW_SIZE = 1_000;
    private static final int SLIDE_INTERVAL = 500;
    private final ScanEventStore events;
    private long publishedThrough;

    public PersistentScanWindow(ScanEventStore events) { this.events = events; }

    @Override
    public void record(String sku) { events.append(Objects.requireNonNull(sku)); }

    @Override
    public Optional<Snapshot> slideIfDue() {
        long end = publishedThrough + SLIDE_INTERVAL;
        if (events.count() < end) return Optional.empty();
        long start = Math.max(1, end - WINDOW_SIZE + 1);
        List<String> scans = events.between(start, end);
        if (scans.size() != end - start + 1) {
            throw new IllegalStateException("Incomplete scan log for window " + end);
        }
        var counts = scans.stream().collect(Collectors.groupingBy(sku -> sku, Collectors.counting()));
        var ranked = counts.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed()
                        .thenComparing(Map.Entry::getKey))
                .map(e -> new SkuCount(e.getKey(), e.getValue())).toList();
        // Do not advance here: a failed persistence attempt must offer this window again.
        return Optional.of(new Snapshot(scans.size(), start, end, ranked));
    }

    @Override
    public void resumeAfter(long sequence) { publishedThrough = sequence; }
    @Override
    public int windowSize() { return WINDOW_SIZE; }
    @Override
    public int slideInterval() { return SLIDE_INTERVAL; }
}
