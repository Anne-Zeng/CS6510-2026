package edu.cs6510.monolith_server.catalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

// Create the first database entity class: the product catalog.
// This tells Spring: “Create a persistent catalog_items table with SKU, 
// name, and price.” The SKU(Stock Keeping Unit) is the unique identifier/code for each product.
@Entity
@Table(name = "catalog_items")
public class CatalogItem {

    @Id
    @Column(nullable = false, updatable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal price;

    protected CatalogItem() {
        // Required by JPA.
    }

    public CatalogItem(String sku, String name, BigDecimal price) {
        this.sku = sku;
        this.name = name;
        this.price = price;
    }

    public String getSku() {
        return sku;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }
}