package edu.cs6510.servicebased.shared.contract;

import java.math.BigDecimal;

/** 功能：收据中的商品明细；同一商品通过 quantity 汇总数量。 */

public record ReceiptLine(
        String sku,
        String name,
        BigDecimal unitPrice,
        int quantity
) {
}
