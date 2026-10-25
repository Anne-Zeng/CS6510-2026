package edu.cs6510.servicebased.shared.contract;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** 功能：完成交易后返回的不可变收据；金额使用 BigDecimal。 */

public record Receipt(
        String transactionId,
        String stationId,
        int itemCount,
        BigDecimal totalAmount,
        Instant startedAt,
        Instant completedAt,
        List<ReceiptLine> lines
) {
}
