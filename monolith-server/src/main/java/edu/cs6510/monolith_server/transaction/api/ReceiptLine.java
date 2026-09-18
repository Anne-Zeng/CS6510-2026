package edu.cs6510.monolith_server.transaction.api;

import java.math.BigDecimal;

// A small, immutable response-data object. A Java record is a shorter way to create a class whose job is only to carry data
// This record represents a line item in a receipt, containing information about a purchased product
// We use a record because this object has no changing behavior—it simply describes one purchased product on a receipt
// It is not a database table/entity. TransactionItem is the persistent database entity; ReceiptLine is the JSON returned to the client.
public record ReceiptLine(
        String sku,
        String name,
        BigDecimal unitPrice,
        int quantity
) {
}