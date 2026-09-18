package edu.cs6510.monolith_server.transaction;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "checkout_transactions")
// Creates the persistent checkout_transactions table. Each new transaction begins as OPEN, 
// with zero scanned items and a $0.00 total. Inventory is still unchanged at this stage.
// When the transaction is completed, the status is set to COMPLETED, and the completedAt timestamp is recorded.
public class CheckoutTransaction {

    // The @Id annotation is used to specify the primary key of the entity. In this case, the transactionId field is marked as the primary key for the CheckoutTransaction entity.
    // The @Column annotation is used to specify the properties of the transactionId field in the checkout_transactions table.
    // The name = "transaction_id" specifies that the column should be named transaction_id, which is consistent with the naming convention used in the CheckoutTransaction entity.
    // The nullable = false indicates that the transactionId field cannot be null, which ensures that each CheckoutTransaction has a unique identifier.
    // The updatable = false indicates that the transactionId field cannot be updated once it is set, which ensures that the primary key remains constant throughout the lifecycle of the entity.
    // The length = 36 specifies that the maximum length of the transactionId field is 36 characters, which is consistent with the length of a UUID string representation.
    @Id
    @Column(name = "transaction_id", nullable = false, updatable = false, length = 36)
    // The transactionId field is used to store the unique identifier for each checkout transaction. It is typically generated as a UUID (Universally Unique Identifier) to ensure uniqueness across all transactions. The transactionId serves as the primary key for the CheckoutTransaction entity, allowing it to be uniquely identified and referenced in the database.
    private String transactionId;

    // The @Column annotation is used to specify the properties of the stationId field in the checkout_transactions table.
    // The nullable = false indicates that the stationId field cannot be null, which ensures that each CheckoutTransaction is associated with a valid station.
    // The stationId field is used to store the identifier of the station where the checkout transaction is taking place. It allows the system to track which
    @Column(nullable = false)
    private String stationId;

    // The @Enumerated annotation is used to specify that the status field should be persisted as an enumerated type in the database. The EnumType.STRING indicates that the enum values should be stored as their string representations (e.g., "OPEN", "COMPLETED", "CANCELLED") rather than their ordinal values (0, 1, 2). This approach improves readability and maintainability of the data in the database.
    // The @Column annotation is used to specify the properties of the status field in the checkout_transactions table. The nullable = false indicates that the status field cannot be null, which ensures that each CheckoutTransaction has a valid status. 
    // The status field is of type TransactionStatus, which is an enum that defines the possible states of a checkout transaction. The enum values are OPEN, COMPLETED, and CANCELLED, representing the different stages of a transaction's lifecycle. By using an enum for the status field, the system can enforce type safety and ensure that only valid status values are assigned to a CheckoutTransaction.
    // The status field is used to store the current status of the checkout transaction. It indicates whether the transaction is still open and in progress (OPEN), has been successfully completed (COMPLETED), or has been cancelled (CANCELLED). The status field allows the system to track the state of each transaction and take appropriate actions based on its current status.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    // The @Column annotation is used to specify the properties of the itemCount field in the checkout_transactions table.
    // The itemCount field is used to store the number of items that have been scanned in the checkout transaction. It is initialized to zero when a new transaction is created and is incremented each time an item is scanned. The itemCount field allows the system to keep track of the total number of items in the transaction and provides a way to calculate the total cost of the transaction based on the scanned items.
    @Column(nullable = false)
    private int itemCount;

    // The @Column annotation is used to specify the properties of the runningTotal field in the checkout_transactions table.
    // The runningTotal field is used to store the total cost of the items that have been scanned in the checkout transaction. It is initialized to $0.00 when a new transaction is created and is updated each time an item is scanned. The runningTotal field allows the system to keep track of the total cost of the transaction and provides a way to calculate the final amount to be charged to the customer when the transaction is completed. 
    // The precision = 10 and scale = 2 specify that the field can store up to 10 digits with 2 decimal places.
    // The runningTotal field is of type BigDecimal, which is a class in Java that provides arbitrary-precision decimal arithmetic. It is used to represent monetary values accurately and avoid rounding errors that can occur with floating-point types. By using BigDecimal for the runningTotal field, the system can ensure that the total cost of the transaction is calculated and stored with high precision, which is important for financial transactions.
    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal runningTotal;

    // The @Column annotation is used to specify the properties of the startedAt field in the checkout_transactions table.
    // The nullable = false indicates that the startedAt field cannot be null, which ensures that each CheckoutTransaction has a valid timestamp indicating when the transaction was initiated. 
    // The startedAt field is of type Instant, which is a class in Java that represents a specific point in time with nanosecond precision. It is used to record the timestamp when the checkout transaction was started, allowing the system to track the duration of the transaction and provide accurate timing information for reporting and analysis purposes.
    @Column(nullable = false)
    private Instant startedAt;

    // The completedAt field is of type Instant, which is a class in Java that represents a specific point in time with nanosecond precision. It is used to record the timestamp when the checkout transaction was completed, allowing the system to track the duration of the transaction and provide accurate timing information for reporting and analysis purposes. 
    // The completedAt field is only set when the transaction is completed, and it remains null for transactions that are still open or have been cancelled. This allows the system to differentiate between completed and incomplete transactions and provides a way to calculate the total time taken for each transaction.
    private Instant completedAt;

    // The protected no-argument constructor is required by JPA (Java Persistence API) for entity classes.
    // JPA uses reflection to create instances of entity classes, and it requires a no-
    protected CheckoutTransaction() {
        // Required by JPA.
    }

    // The public constructor is used to create a new instance of the CheckoutTransaction class with the specified transactionId, stationId, and startedAt timestamp.
    // It initializes the transactionId, stationId, and startedAt fields with the provided values, and sets the status field to OPEN, the itemCount field to 0, and the runningTotal field to $0.00, indicating that the transaction is in progress and no items have been scanned yet. 
    // This constructor is typically called when a new checkout transaction is initiated, and it ensures that the CheckoutTransaction is properly initialized with the relevant information.
    // The constructor takes the following parameters:
    // - String transactionId: The unique identifier for the checkout transaction, typically generated as a UUID (Universally Unique Identifier) to ensure uniqueness across all transactions.
    // - String stationId: The identifier of the station where the checkout transaction is taking place, allowing the system to track which station is processing the transaction.
    // - Instant startedAt: The timestamp indicating when the checkout transaction was initiated, allowing the system to track the duration of the transaction and provide accurate timing information for reporting and analysis purposes.
    public CheckoutTransaction(String transactionId, String stationId, Instant startedAt) {
        this.transactionId = transactionId;
        this.stationId = stationId;
        this.status = TransactionStatus.OPEN;
        this.itemCount = 0;
        this.runningTotal = new BigDecimal("0.00");
        this.startedAt = startedAt;
    }
    // The addScannedItem() method is used to update the itemCount and runningTotal fields when a new item is scanned in the checkout transaction.
    // It takes the unitPrice of the scanned item as a parameter and increments the itemCount by 1 and adds the unitPrice to the runningTotal. This method is typically called each time an item is scanned during the checkout process, allowing the system to keep track of the total number of items and the total cost of the transaction in real-time. By updating the itemCount and runningTotal fields, the system can provide accurate information to the customer and ensure that the final amount charged reflects the total cost of all scanned items.
    public void addScannedItem(BigDecimal unitPrice) {
        this.itemCount++;
        this.runningTotal = this.runningTotal.add(unitPrice);
    }

    // The complete() method is used to mark the checkout transaction as completed and record the completedAt timestamp.
    // It takes the completedAt timestamp as a parameter and sets the status field to COMPLETED and the completedAt field to the provided timestamp. This method is typically called when the checkout process is finished and the transaction is finalized, allowing the system to track the completion of the transaction and provide accurate timing information for reporting and analysis purposes. By marking the transaction as completed, the system can differentiate between completed and incomplete transactions and ensure that the final amount charged reflects the total cost of all scanned items.
    public void complete(Instant completedAt) {
        this.status = TransactionStatus.COMPLETED;
        this.completedAt = completedAt;
    }

    // // The cancel() method is used to mark the checkout transaction as cancelled.
    // // It sets the status field to CANCELLED, indicating that the transaction has been aborted and
    // public void cancel() {
    //     this.status = TransactionStatus.CANCELLED;
    // }

    // The following getter methods are used to retrieve the values of the private fields in the CheckoutTransaction class.
    // These methods provide read-only access to the fields, allowing other classes to access the values without directly modifying them.
    // The getTransactionId() method returns the unique identifier of the CheckoutTransaction.
    // The getStationId() method returns the identifier of the station where the checkout transaction is taking place.
    // The getStatus() method returns the current status of the CheckoutTransaction,
    // The getItemCount() method returns the number of items that have been scanned in the checkout transaction.
    // The getRunningTotal() method returns the total cost of the items that have been scanned in the checkout transaction.
    // The getStartedAt() method returns the timestamp indicating when the checkout transaction was initiated.
    // The getCompletedAt() method returns the timestamp indicating when the checkout transaction was completed, or null if the transaction is still open or has been cancelled.
    // These getter methods are important for encapsulation, as they allow controlled access to the private fields of the
    public String getTransactionId() {
        return transactionId;
    }

    public String getStationId() {
        return stationId;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public int getItemCount() {
        return itemCount;
    }

    public BigDecimal getRunningTotal() {
        return runningTotal;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}