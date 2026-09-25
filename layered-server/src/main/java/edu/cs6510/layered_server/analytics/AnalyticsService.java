package edu.cs6510.layered_server.analytics;

import edu.cs6510.layered_server.common.BusinessException;
import edu.cs6510.layered_server.contract.PopularItemsResponse;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** Windowing and ranking policy, independent of transaction business rules. */
@Service
public class AnalyticsService {
    public static final int TOP_ITEM_LIMIT = 10;
    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);
    private final ScanWindow scanWindow;
    private final AnalyticsWindowStore store;

    public AnalyticsService(ScanWindow scanWindow, AnalyticsWindowStore store) {
        this.scanWindow = scanWindow;
        this.store = store;
    }

    @PostConstruct
    void resumeWindow() { scanWindow.resumeAfter(store.lastPublishedSequence()); }

    /** Called within the API scan use-case transaction; failures roll back the scan. */
    public void recordScan(String sku) { scanWindow.record(sku); }

    @Scheduled(fixedDelayString = "${analytics.window.flush-ms:25}")
    public synchronized void publishWindowIfDue() {
        try {
            // Bounded batches catch up without dropping intermediate windows.
            for (int i = 0; i < 100; i++) {
                var next = scanWindow.slideIfDue();
                if (next.isEmpty()) return;
                var snapshot = next.get();
                store.save(snapshot, scanWindow.windowSize(), scanWindow.slideInterval(), TOP_ITEM_LIMIT);
                // save() returns through its transaction proxy only after commit.
                scanWindow.resumeAfter(snapshot.lastSequence());
            }
        } catch (RuntimeException e) {
            log.warn("Analytics publication failed; the same window will be retried", e);
        }
    }

    public PopularItemsResponse getPopularItems(Integer requestedLimit) {
        int limit = requestedLimit == null ? TOP_ITEM_LIMIT : requestedLimit;
        if (limit < 1) {
            throw new BusinessException(BusinessException.Kind.INVALID_REQUEST,
                    "INVALID_REQUEST", "limit must be greater than zero");
        }
        return store.latestRanking(limit, scanWindow.windowSize(), scanWindow.slideInterval());
    }
}
