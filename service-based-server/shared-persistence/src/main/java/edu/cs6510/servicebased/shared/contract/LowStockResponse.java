package edu.cs6510.servicebased.shared.contract;

import java.time.Instant;
import java.util.List;

/** 功能：低库存查询响应：包含查询阈值、生成时间和警报列表。 */

public record LowStockResponse(
        int threshold,
        Instant generatedAt,
        List<LowStockAlert> alerts
) {
}
