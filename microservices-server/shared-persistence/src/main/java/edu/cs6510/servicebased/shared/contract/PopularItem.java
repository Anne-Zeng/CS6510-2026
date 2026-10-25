package edu.cs6510.servicebased.shared.contract;

/** 功能：热门商品的一行排名：SKU、名称、扫描次数和名次。 */

public record PopularItem(
        String sku,
        String name,
        int scanCount,
        int rank
) {
}
