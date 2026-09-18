package edu.cs6510.monolith_server.analytics;

import edu.cs6510.monolith_server.analytics.api.PopularItem;
import edu.cs6510.monolith_server.analytics.api.PopularItemsResponse;
import edu.cs6510.monolith_server.catalog.CatalogItem;
import edu.cs6510.monolith_server.catalog.CatalogItemRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class AnalyticsService {

    public static final int WINDOW_SIZE = 1_000;
    public static final int SLIDE_INTERVAL = 500;
    public static final int TOP_ITEM_LIMIT = 10;

    private final ScanEventRepository scanEventRepository;
    private final AnalyticsWindowRepository analyticsWindowRepository;
    private final PopularItemResultRepository popularItemResultRepository;
    private final CatalogItemRepository catalogItemRepository;

    public AnalyticsService(
            ScanEventRepository scanEventRepository,
            AnalyticsWindowRepository analyticsWindowRepository,
            PopularItemResultRepository popularItemResultRepository,
            CatalogItemRepository catalogItemRepository
    ) {
        this.scanEventRepository = scanEventRepository;
        this.analyticsWindowRepository = analyticsWindowRepository;
        this.popularItemResultRepository = popularItemResultRepository;
        this.catalogItemRepository = catalogItemRepository;
    }

    @Transactional
    public void recordScan(String sku) {
        scanEventRepository.saveAndFlush(new ScanEvent(sku, Instant.now()));

        long totalScans = scanEventRepository.count();

        if (totalScans % SLIDE_INTERVAL == 0) {
            calculateAndPersistLatestWindow();
        }
    }

    @Transactional(readOnly = true)
    public PopularItemsResponse getPopularItems(Integer requestedLimit) {
        int limit = requestedLimit == null ? TOP_ITEM_LIMIT : requestedLimit;

        if (limit < 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "limit must be greater than zero"
            );
        }

        return analyticsWindowRepository
                .findTopByOrderByWindowEndDesc()
                .map(window -> {
                    List<PopularItem> items = popularItemResultRepository
                            .findAllByWindowIdOrderByRankAsc(window.getId())
                            .stream()
                            .limit(limit)
                            .map(result -> new PopularItem(
                                    result.getSku(),
                                    result.getName(),
                                    result.getScanCount(),
                                    result.getRank()
                            ))
                            .toList();

                    return new PopularItemsResponse(
                            window.getWindowSize(),
                            window.getSlideInterval(),
                            window.getWindowStart(),
                            window.getWindowEnd(),
                            window.getComputedAt(),
                            items
                    );
                })
                .orElseGet(() -> new PopularItemsResponse(
                        WINDOW_SIZE,
                        SLIDE_INTERVAL,
                        0,
                        0,
                        Instant.now(),
                        List.of()
                ));
    }

    private void calculateAndPersistLatestWindow() {
        List<ScanEvent> latestScans =
                scanEventRepository.findTop1000ByOrderBySequenceNumberDesc();

        if (latestScans.isEmpty()) {
            return;
        }

        Map<String, Long> scanCounts = latestScans.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        ScanEvent::getSku,
                        java.util.stream.Collectors.counting()
                ));

        List<Map.Entry<String, Long>> rankedItems = scanCounts.entrySet()
                .stream()
                .sorted(
                        Comparator
                                .comparingLong(
                                        (Map.Entry<String, Long> entry) -> entry.getValue()
                                )
                                .reversed()
                                .thenComparing(Map.Entry::getKey)
                )
                .limit(TOP_ITEM_LIMIT)
                .toList();

        long windowStart = latestScans.get(latestScans.size() - 1)
                .getSequenceNumber();

        long windowEnd = latestScans.get(0)
                .getSequenceNumber();

        AnalyticsWindow window = analyticsWindowRepository.saveAndFlush(
                new AnalyticsWindow(
                        latestScans.size(),
                        SLIDE_INTERVAL,
                        windowStart,
                        windowEnd,
                        Instant.now()
                )
        );

        int rank = 1;
        for (Map.Entry<String, Long> rankedItem : rankedItems) {
            CatalogItem catalogItem = catalogItemRepository
                    .findById(rankedItem.getKey())
                    .orElseThrow(() -> new ResponseStatusException(
                            HttpStatus.NOT_FOUND,
                            "Catalog SKU not found"
                    ));

            popularItemResultRepository.save(
                    new PopularItemResult(
                            window,
                            catalogItem.getSku(),
                            catalogItem.getName(),
                            rankedItem.getValue().intValue(),
                            rank
                    )
            );

            rank++;
        }
    }
}