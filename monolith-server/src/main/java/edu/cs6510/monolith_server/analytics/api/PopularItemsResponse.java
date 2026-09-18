package edu.cs6510.monolith_server.analytics.api;

import java.time.Instant;
import java.util.List;

public record PopularItemsResponse(
        int windowSize,
        int slideInterval,
        long windowStart,
        long windowEnd,
        Instant computedAt,
        List<PopularItem> items
) {
}