package edu.cs6510.monolith_server.transaction.api;

import java.math.BigDecimal;

// ScanResult.java describes what your server sends back:
// The ScanResult record is a simple data transfer object (DTO) that represents the result of scanning an item during a checkout transaction.
// It contains the following fields:
// - String transactionId: The unique identifier for the checkout transaction, allowing the system to associate the scanned item with the correct transaction.
// - String sku: The Stock Keeping Unit (SKU) of the scanned item, which is a unique identifier used to track inventory and manage product information.
// - String name: The name of the scanned item, providing a human-readable description of the product.
// - BigDecimal unitPrice: The price of a single unit of the scanned item, allowing the system to calculate the total cost of the transaction based on the quantity of items scanned.
// - int itemCount: The total number of units of the scanned item that have been processed in the transaction, allowing the system to track the quantity of each product being purchased.
// - BigDecimal runningTotal: The cumulative total cost of all scanned items in the transaction, allowing the system to provide real-time feedback on the total amount due for the transaction. This field is updated each time a new item is scanned, ensuring that the customer and the system have an accurate representation of the total cost of the transaction at any given time.
// The ScanResult record is used to send the result of a scan operation back to the client, providing the scanned item's details and the current state of the transaction.
public record ScanResult(
        String transactionId,
        String sku,
        String name,
        BigDecimal unitPrice,
        int itemCount,
        BigDecimal runningTotal
) {
}