package edu.cs6510.monolith_server.transaction.api;

import edu.cs6510.monolith_server.transaction.CheckoutTransaction;
import edu.cs6510.monolith_server.transaction.TransactionStatus;

import java.math.BigDecimal;
import java.time.Instant;

// The TransactionResponse record is a simple data transfer object (DTO) that represents the response payload for a checkout transaction.
// This record is typically used to send transaction information back to the client after a transaction has been initiated or completed, providing a clear and structured representation of the transaction's state and details.
// The fields in the TransactionResponse record are as follows:
// - String transactionId: The unique identifier for the checkout transaction, typically generated as a UUID (Universally Unique Identifier) to ensure uniqueness across all transactions.
// - String stationId: The identifier of the station where the checkout transaction is taking place, allowing the system to track which station is processing the transaction.
// - TransactionStatus status: The current status of the checkout transaction, represented by the TransactionStatus enum, which can be OPEN, COMPLETED, or CANCELLED.
// - int itemCount: The total number of items that have been scanned in the checkout transaction, allowing the system to track the quantity of items being processed.
// - BigDecimal runningTotal: The total cost of all scanned items in the checkout transaction, represented as a BigDecimal to ensure precision in monetary calculations.
// - Instant startedAt: The timestamp indicating when the checkout transaction was initiated, allowing the system to track the duration of the transaction and provide accurate timing information for reporting and analysis purposes.
// The from() method(a static factory method) takes a CheckoutTransaction entity as a parameter and extracts the relevant information to create a new TransactionResponse instance, providing a convenient way to convert between the entity and the response DTO.
public record TransactionResponse(
        String transactionId,
        String stationId,
        TransactionStatus status,
        int itemCount,
        BigDecimal runningTotal,
        Instant startedAt
) {
    public static TransactionResponse from(CheckoutTransaction transaction) {
        return new TransactionResponse(
                transaction.getTransactionId(),
                transaction.getStationId(),
                transaction.getStatus(),
                transaction.getItemCount(),
                transaction.getRunningTotal(),
                transaction.getStartedAt()
        );
    }
}