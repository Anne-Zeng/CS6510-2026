package edu.cs6510.monolith_server.transaction.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

// Receipt is the complete payment result, while ReceiptLine is one item within it.
// Receipt uses a record for the same reason: once payment is complete, it is an unchanging data response sent back as JSON.
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