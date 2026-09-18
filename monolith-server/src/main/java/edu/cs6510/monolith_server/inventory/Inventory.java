package edu.cs6510.monolith_server.inventory;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

//Create the database entity classes: first CatalogItem, then Inventory.
//Inventory.java = one inventory record/the inventory data model.
// This tells Spring: “Create a persistent inventory table with SKU and currentStock.”
// Inventory uses the same SKU as CatalogItem. That SKU connects a product’s name/
// price in catalog_items to its current quantity in the inventory table.
@Entity
@Table(name = "inventory")
public class Inventory {

    @Id
    @Column(nullable = false, updatable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private int currentStock;

    @Column(name = "low_stock_triggered_at")
    private Instant lowStockTriggeredAt;

    protected Inventory() {
        // Required by JPA.
    }

    public Inventory(String sku, int currentStock) {
        this.sku = sku;
        this.currentStock = currentStock;
    }

    public void markLowStock(Instant triggeredAt) {
        if (this.lowStockTriggeredAt == null) {
            this.lowStockTriggeredAt = triggeredAt;
        }
    }

    public String getSku() {
        return sku;
    }

    public int getCurrentStock() {
        return currentStock;
    }

    public Instant getLowStockTriggeredAt() {
        return lowStockTriggeredAt;
    }
}