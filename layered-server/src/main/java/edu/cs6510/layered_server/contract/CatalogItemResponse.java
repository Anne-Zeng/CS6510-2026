package edu.cs6510.layered_server.contract;

import java.math.BigDecimal;

public record CatalogItemResponse(
        String sku,
        String name,
        BigDecimal price
) {
}
