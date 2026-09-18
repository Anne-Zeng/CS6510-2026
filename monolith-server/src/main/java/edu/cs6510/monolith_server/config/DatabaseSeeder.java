package edu.cs6510.monolith_server.config;

import edu.cs6510.monolith_server.catalog.CatalogItem;
import edu.cs6510.monolith_server.catalog.CatalogItemRepository;
import edu.cs6510.monolith_server.inventory.Inventory;
import edu.cs6510.monolith_server.inventory.InventoryRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

// The DatabaseSeeder class is a Spring component that implements CommandLineRunner.
// It is responsible for seeding the database with initial data when the application starts.

// Runs automatically whenever Spring server starts. If products already exist, it does nothing.
// If the database is empty, it creates:- SKU-000001 through SKU-002000:
// - a name and price for each item
// - 10,000 inventory units for every SKU

// The @Component annotation indicates that this class is a Spring-managed component, allowing it to be automatically detected and registered as a bean in the application context.
@Component
public class DatabaseSeeder implements CommandLineRunner {
    // The CATALOG_SIZE constant defines the number of catalog items to be created, and the INITIAL_STOCK constant defines the initial stock quantity for each item in the inventory.
    // The CatalogItemRepository and InventoryRepository are used to interact with the catalog_items and inventory tables in the database, respectively.
    // The DatabaseSeeder class uses these repositories to save the generated catalog items and inventory items to the database.
    private static final int CATALOG_SIZE = 2_000;
    private static final int INITIAL_STOCK = 10_000;
    // The CatalogItemRepository and InventoryRepository are injected into the DatabaseSeeder class through its constructor, allowing it to interact with the database and perform CRUD operations on the catalog_items and inventory tables.
    private final CatalogItemRepository catalogItemRepository;
    private final InventoryRepository inventoryRepository;

    // The constructor of the DatabaseSeeder class takes two parameters: CatalogItemRepository and InventoryRepository. 
    // These repositories are used to interact with the catalog_items and inventory tables in the database, respectively.
    public DatabaseSeeder(
            CatalogItemRepository catalogItemRepository,
            InventoryRepository inventoryRepository
    ) {
        this.catalogItemRepository = catalogItemRepository;
        this.inventoryRepository = inventoryRepository;
    }

    @Override
    @Transactional // The @Transactional annotation ensures that the database operations performed in the run method are executed within a transaction, providing atomicity and consistency.
    // The run method checks if the catalog already exists in the database. If it does, it skips the seeding process. Otherwise, it generates a list of CatalogItem and Inventory objects, populates them with sample data, and saves them to the respective repositories.
    public void run(String... args) {
        if (catalogItemRepository.count() > 0) {
            System.out.println("Catalog already exists; skipping database seed.");
            return;
        }
        // The run method generates a list of CatalogItem and Inventory objects, populates them with sample data, and saves them to the respective repositories.
        // It uses a loop to create 2,000 catalog items with unique SKUs, names, and prices, and initializes the inventory for each item with 10,000 units.
        // The generated catalog items and inventory items are then saved to the database using the saveAll method of the respective repositories.
        // The run method prints a message indicating that the database has been seeded with the specified number of catalog items and inventory units.
        List<CatalogItem> catalogItems = new ArrayList<>();
        List<Inventory> inventoryItems = new ArrayList<>();
        for (int itemNumber = 1; itemNumber <= CATALOG_SIZE; itemNumber++) {
            String sku = String.format("SKU-%06d", itemNumber);
            String name = String.format("Store Item %06d", itemNumber);
            BigDecimal price = BigDecimal.valueOf(100 + (itemNumber % 1_000), 2);

            catalogItems.add(new CatalogItem(sku, name, price));
            inventoryItems.add(new Inventory(sku, INITIAL_STOCK));
        }

        catalogItemRepository.saveAll(catalogItems);
        inventoryRepository.saveAll(inventoryItems);

        System.out.println("Seeded 2,000 catalog items with 10,000 units each.");
    }
}
