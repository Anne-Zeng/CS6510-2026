package edu.cs6510.layered_server.analytics;

import edu.cs6510.layered_server.contract.PopularItem;
import edu.cs6510.layered_server.contract.PopularItemsResponse;
import edu.cs6510.layered_server.persistence.AnalyticsWindowRepository;
import edu.cs6510.layered_server.persistence.CatalogCache;
import edu.cs6510.layered_server.persistence.PopularItemResultRepository;
import edu.cs6510.layered_server.persistence.entity.AnalyticsWindow;
import edu.cs6510.layered_server.persistence.entity.PopularItemResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * All of the analytics layer's database access, in one place.
 *
 * <p>It is a separate bean from {@link AnalyticsService} rather than a method on
 * it because Spring's {@code @Transactional} is implemented with a proxy: a
 * method the service calls on itself would bypass that proxy and silently run
 * with no transaction at all.
 */
@Component
public class AnalyticsWindowStore {

    private final AnalyticsWindowRepository windows;
    private final PopularItemResultRepository rankings;
    private final CatalogCache catalog;

    public AnalyticsWindowStore(AnalyticsWindowRepository windows,
                                PopularItemResultRepository rankings,
                                CatalogCache catalog) {
        this.windows = windows;
        this.rankings = rankings;
        this.catalog = catalog;
    }

    /**
     * The end sequence of the newest durably published window, or 0 if none has
     * been published. This is the analytics layer's restart checkpoint.
     */
    @Transactional(readOnly = true)
    public long lastPublishedSequence() {
        return windows.findTopByOrderByWindowEndDesc()
                .map(AnalyticsWindow::getWindowEnd)
                .orElse(0L);
    }

    /** Writes one computed window and its ranking as a single atomic unit. */
    @Transactional
    public void save(ScanWindow.Snapshot snapshot, int windowSize, int slideInterval, int topLimit) {
        // Idempotent after a commit whose acknowledgement was interrupted.
        if (windows.existsByWindowEnd(snapshot.lastSequence())) return;
        AnalyticsWindow window = windows.save(new AnalyticsWindow(
                windowSize, slideInterval,
                snapshot.firstSequence(), snapshot.lastSequence(), Instant.now()));

        List<ScanWindow.SkuCount> top = snapshot.ranked().stream().limit(topLimit).toList();
        List<PopularItemResult> results = new ArrayList<>(top.size());
        for (int index = 0; index < top.size(); index++) {
            ScanWindow.SkuCount entry = top.get(index);
            // A SKU can only enter a window by being scanned, and a scan only
            // succeeds for a catalogued SKU, so a miss here means corrupt data.
            String name = catalog.find(entry.sku())
                    .orElseThrow(() -> new IllegalStateException("Uncatalogued SKU in window: " + entry.sku()))
                    .getName();
            results.add(new PopularItemResult(window, entry.sku(), name, (int) entry.count(), index + 1));
        }
        rankings.saveAll(results);
    }

    /**
     * Returns the most recently published ranking, or an empty one when no
     * window has been published yet.
     */
    @Transactional(readOnly = true)
    public PopularItemsResponse latestRanking(int limit, int windowSize, int slideInterval) {
        Optional<AnalyticsWindow> latest = windows.findTopByOrderByWindowEndDesc();
        if (latest.isEmpty()) {
            return new PopularItemsResponse(windowSize, slideInterval, 0, 0, Instant.now(), List.of());
        }

        AnalyticsWindow window = latest.get();
        // Only the top ten are persisted, so a larger limit still returns at most
        // those ten; a smaller limit returns their prefix.
        List<PopularItem> items = rankings.findAllByWindowIdOrderByRankAsc(window.getId()).stream()
                .limit(limit)
                .map(row -> new PopularItem(row.getSku(), row.getName(), row.getScanCount(), row.getRank()))
                .toList();

        return new PopularItemsResponse(window.getWindowSize(), window.getSlideInterval(),
                window.getWindowStart(), window.getWindowEnd(), window.getComputedAt(), items);
    }
}
