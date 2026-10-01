package edu.cs6510.pipeline_server.contract;

import java.time.Instant;
import java.util.List;

public record LowStockResponse(
        int threshold,
        Instant generatedAt,
        List<LowStockAlert> alerts
) {
}
