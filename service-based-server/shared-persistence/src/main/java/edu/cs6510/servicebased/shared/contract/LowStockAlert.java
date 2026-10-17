package edu.cs6510.servicebased.shared.contract;

import java.time.Instant;

/** 功能：库存警报数据：商品、当前库存、阈值及首次触发时间。 */

public record LowStockAlert(
        String sku,
        String name,
        int currentStock,
        int threshold,
        Instant triggeredAt
) {
}
