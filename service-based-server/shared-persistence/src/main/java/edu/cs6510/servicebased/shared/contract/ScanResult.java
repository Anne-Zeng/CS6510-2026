package edu.cs6510.servicebased.shared.contract;

import java.math.BigDecimal;

/** 功能：扫描结果：商品信息及整张购物篮的累计件数、金额。 */

public record ScanResult(
        String transactionId,
        String sku,
        String name,
        BigDecimal unitPrice,
        int itemCount,
        BigDecimal runningTotal
) {
}
