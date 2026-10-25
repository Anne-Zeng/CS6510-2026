package edu.cs6510.servicebased.shared.contract;

import java.time.Instant;
import java.util.List;

/** 功能：热门商品响应：包括滑动窗口边界和已持久化的前十名。 */

public record PopularItemsResponse(
        int windowSize,
        int slideInterval,
        long windowStart,
        long windowEnd,
        Instant computedAt,
        List<PopularItem> items
) {
}
