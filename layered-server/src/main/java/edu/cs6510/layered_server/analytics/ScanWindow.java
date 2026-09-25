package edu.cs6510.layered_server.analytics;

import java.util.List;
import java.util.Optional;


//An interface: it defines operations such as “record this scan” and “give me
// a window when it’s ready.” The implementation, currently PersistentScanWindow,
// contains the details of how those operations work.
/**
 * The windowing algorithm, isolated behind an interface so it can be replaced
 * without touching the API layer or the transactions layer. A count-based
 * sliding window is one choice; a time-based tumbling window, an approximate
 * heavy-hitters sketch, or a Kafka-backed stream would all implement this same
 * contract.
 *
 * <p>Implementations must make {@link #record} cheap, because it runs once per
 * scan on the hottest path in the system. Ranking work belongs in
 * {@link #slideIfDue}, which the analytics layer calls off the request path.
 */
public interface ScanWindow {

    /** Observes one scanned SKU. Called once per scan; must not block on ranking work. */
    void record(String sku);

    /**
     * Returns a ranked snapshot when enough scans have accumulated for the
     * window to advance, and {@link Optional#empty()} when it has not. Never
     * called from a request thread.
     */
    Optional<Snapshot> slideIfDue();

    /**
     * Tells the window that everything up to {@code lastPublishedSequence} has
     * already been published durably, so {@link #slideIfDue()} must not offer it
     * again. Called once at startup.
     *
     * <p>Without this, an implementation that tracks its progress in memory
     * would restart believing nothing had ever been published and immediately
     * re-rank whatever scans were still buffered, producing a duplicate window
     * that disagrees with the one already persisted.
     */
    void resumeAfter(long lastPublishedSequence);

    /** Nominal number of scans held in one window. */
    int windowSize();

    /** How many new scans must arrive before the window advances. */
    int slideInterval();

    /**
     * One computed window. {@code firstSequence} and {@code lastSequence} are
     * positions in the global scan stream, so a partial first window is
     * distinguishable from a full one.
     */
    record Snapshot(int observedScans, long firstSequence, long lastSequence, List<SkuCount> ranked) {}

    /** A SKU and how many times it appeared in the window. */
    record SkuCount(String sku, long count) {}
}
