package edu.cs6510.monolith_server.inventory.api;

import java.time.Instant;
import java.util.List;

public record LowStockResponse(
        int threshold,
        Instant generatedAt,
        List<LowStockAlert> alerts
) {
}