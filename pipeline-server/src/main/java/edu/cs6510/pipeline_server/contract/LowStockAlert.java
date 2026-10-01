package edu.cs6510.pipeline_server.contract;

import java.time.Instant;

public record LowStockAlert(
        String sku,
        String name,
        int currentStock,
        int threshold,
        Instant triggeredAt
) {
}
