package edu.cs6510.monolith_server.catalog.api;

import java.math.BigDecimal;

public record CatalogItemResponse(
        String sku,
        String name,
        BigDecimal price
) {
}