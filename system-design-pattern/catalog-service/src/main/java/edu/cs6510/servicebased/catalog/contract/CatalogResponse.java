package edu.cs6510.servicebased.catalog.contract;

import java.util.List;

/** 返回给调用方的数据格式，定义整个目录的响应：封装按 SKU 排序的商品列表。 */

public record CatalogResponse(
        List<CatalogItemResponse> items
) {
}
