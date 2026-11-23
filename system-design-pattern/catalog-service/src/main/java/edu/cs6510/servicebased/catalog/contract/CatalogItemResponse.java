package edu.cs6510.servicebased.catalog.contract;

import java.math.BigDecimal;

/** 返回给调用方的数据格式, 定义单个商品的响应字段: SKU、名称和精确到分的单价。 */

public record CatalogItemResponse(
        String sku,
        String name,
        BigDecimal price
) {
}
