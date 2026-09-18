package edu.cs6510.monolith_server.inventory.api;

import java.time.Instant;

public record LowStockAlert(
        String sku,
        String name,
        int currentStock,
        int threshold,
        Instant triggeredAt
) {
}