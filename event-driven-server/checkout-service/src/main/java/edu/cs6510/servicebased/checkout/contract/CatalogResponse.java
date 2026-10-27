package edu.cs6510.servicebased.checkout.contract;

import java.util.List;

/** 功能：商品目录响应：封装按 SKU 排序的商品列表。 */

public record CatalogResponse(
        List<CatalogItemResponse> items
) {
}
