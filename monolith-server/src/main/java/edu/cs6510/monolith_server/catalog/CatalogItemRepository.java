package edu.cs6510.monolith_server.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

// This interface is a Spring Data JPA repository for CatalogItem entities. 
// It provides CRUD operations and query methods for the catalog_items table in the database.

// The JpaRepository interface is a generic interface that takes two parameters: the entity type (CatalogItem) and the entity's primary key (String, SKU).
// By extending JpaRepository, CatalogItemRepository inherits several methods for working with CatalogItem persistence, including methods for saving, deleting, and finding CatalogItem entities.
// The @Repository annotation is not strictly necessary here because Spring Data JPA will automatically implement this interface and create a bean for it, but it can be added for clarity.
// The CatalogItemRepository interface allows for easy interaction with the catalog_items table in the database, enabling developers to perform CRUD operations and custom queries without having to write boilerplate code.

// JpaRepository<..., String> tells Spring:
// - the first type is the data entity (CatalogItem or Inventory)
// - the second type is its ID type (String because SKUs are text)
public interface CatalogItemRepository extends JpaRepository<CatalogItem, String> {
    List<CatalogItem> findAllByOrderBySkuAsc();
}