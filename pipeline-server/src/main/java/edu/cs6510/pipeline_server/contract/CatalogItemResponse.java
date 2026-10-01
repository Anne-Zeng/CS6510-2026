package edu.cs6510.pipeline_server.contract;

import java.math.BigDecimal;

public record CatalogItemResponse(
        String sku,
        String name,
        BigDecimal price
) {
}
