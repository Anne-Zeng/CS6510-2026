package edu.cs6510.monolith_server.transaction;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

// The TransactionItemRepository interface extends the JpaRepository interface, which provides CRUD (Create, Read, Update, Delete) operations for the TransactionItem entity.
// By extending JpaRepository, the TransactionItemRepository inherits several methods for working with TransactionItem persistence, including methods for saving, deleting, and finding TransactionItem entities. 
// The generic parameters specify that the repository will manage TransactionItem entities and that the ID type of the entity is Long.
// This allows for easy interaction with the database and simplifies data access in the application.
public interface TransactionItemRepository
        extends JpaRepository<TransactionItem, Long> {

    // The findByTransactionTransactionIdAndSku() method is a custom query method that retrieves a TransactionItem entity based on the transaction ID and SKU (Stock Keeping Unit) of the item.
    // It takes two parameters: transactionId (the unique identifier of the transaction) and sku (the unique identifier of the item). 
    // The method returns an Optional<TransactionItem>, which may contain the found TransactionItem or be empty if no matching item is found. This method allows for efficient retrieval of specific items within a transaction, enabling the system to check if an item has already been scanned or processed in the transaction.
    Optional<TransactionItem> findByTransactionTransactionIdAndSku(
            String transactionId,
            String sku
    );
    
    // The findAllByTransactionTransactionId() method is a custom query method that retrieves all TransactionItem entities associated with a specific transaction ID.
    // It takes a single parameter: transactionId (the unique identifier of the transaction).
    // The method returns a List<TransactionItem> containing all items that belong to the specified transaction. This method allows for efficient retrieval of all items within a transaction, enabling the system to display or process the complete list of items that have been scanned or processed in the transaction.
    List<TransactionItem> findAllByTransactionTransactionId(String transactionId);
}