package edu.cs6510.monolith_server.inventory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

// InventoryRepository.java = database access layer for Inventory records/model
// By extending JpaRepository, the InventoryRepository inherits several methods for working with Inventory persistence, including methods for saving, deleting, and finding Inventory entities. 
// The generic parameters specify that the repository will manage Inventory entities and that the ID type of the entity is String. This allows for easy interaction with the database and simplifies data access in the application.
// The decrementIfEnough() method is a custom query method that decrements the current stock of an inventory item if there is enough stock available.
// It takes two parameters: sku (the unique identifier of the item) and quantity (the number of units to decrement). The method uses a JPQL update query to decrement the current stock of the inventory item with the specified SKU, but only if the current stock is greater than or equal to the specified quantity. The method returns an integer indicating the number of rows affected by the update operation, which can be used to determine if the decrement was successful (i.e., if there was enough stock available to fulfill the request). This method allows for efficient stock management and helps prevent overselling of inventory items.
// This is the important concurrency protection: it reduces stock only when enough stock exists. It returns 1 if successful or 0 if the SKU is missing/does not have enough stock.
public interface InventoryRepository extends JpaRepository<Inventory, String> {
    @Modifying(flushAutomatically = true)
    @Query("""
            update Inventory inventory
            set inventory.currentStock = inventory.currentStock - :quantity
            where inventory.sku = :sku
              and inventory.currentStock >= :quantity
            """)
    
    // This method returns the number of inventory rows that were updated:
    // The SQL update only matches an inventory row when both conditions are true: where...and...
    // Then in CheckoutService this line receives that result:
    // int rowsUpdated = inventoryRepository.decrementIfEnough(
        //item.getSku(),
        //item.getQuantity())
    // can check if the return value is 1 (enough stock) or 0 (insufficient stock).
    // when completing a transaction, CheckoutService asks InventoryRepository:
    //“Reduce this SKU’s stock only if enough units remain.”
    int decrementIfEnough(
            @Param("sku") String sku,
            @Param("quantity") int quantity
    );

    List<Inventory> findAllByCurrentStockLessThanEqualOrderByCurrentStockAsc(
            int currentStock
    );
}