package edu.cs6510.servicebased.shared.contract;

import java.math.BigDecimal;

/** 功能：商品目录中的一项：SKU、名称和精确到分的单价。 */

public record CatalogItemResponse(
        String sku,
        String name,
        BigDecimal price
) {
}
