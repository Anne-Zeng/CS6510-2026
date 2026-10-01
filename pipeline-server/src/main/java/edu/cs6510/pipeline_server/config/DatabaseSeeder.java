package edu.cs6510.pipeline_server.config;

import edu.cs6510.pipeline_server.persistence.CatalogItemRepository;
import edu.cs6510.pipeline_server.persistence.InventoryRepository;
import edu.cs6510.pipeline_server.persistence.entity.CatalogItem;
import edu.cs6510.pipeline_server.persistence.entity.Inventory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Seeds the catalog and its opening stock. Both are configurable for the same
 * reason the reference server takes them as arguments
 * ({@code ./run.sh 8080 2000 10000 50}): the opening stock has to outlast the
 * workload, or the benchmark stops measuring the server and starts measuring
 * the fixture. See README for why the default is not 10,000.
 */
@Component
public class DatabaseSeeder implements CommandLineRunner {

    private final CatalogItemRepository catalog;
    private final InventoryRepository inventory;
    private final int catalogSize;
    private final int stockPerItem;

    public DatabaseSeeder(CatalogItemRepository catalog,
                          InventoryRepository inventory,
                          @Value("${checkout.catalog-size:2000}") int catalogSize,
                          @Value("${checkout.stock-per-item:1000000}") int stockPerItem) {
        if (catalogSize < 1 || stockPerItem < 1) {
            throw new IllegalArgumentException("Catalog size and stock per item must be positive");
        }
        this.catalog = catalog;
        this.inventory = inventory;
        this.catalogSize = catalogSize;
        this.stockPerItem = stockPerItem;
    }

    @Override
    @Transactional
    public void run(String... args) {
        long catalogCount = catalog.count();
        long inventoryCount = inventory.count();
        if (catalogCount == catalogSize && inventoryCount == catalogSize) return;
        if (catalogCount != 0 || inventoryCount != 0) {
            throw new IllegalStateException("Incomplete seed data. Stop the server and reset its database.");
        }
        List<CatalogItem> items = new ArrayList<>(catalogSize);
        List<Inventory> stock = new ArrayList<>(catalogSize);
        for (int number = 1; number <= catalogSize; number++) {
            String sku = String.format(Locale.ROOT, "SKU-%06d", number);
            items.add(new CatalogItem(sku, String.format(Locale.ROOT, "Store Item %06d", number),
                    BigDecimal.valueOf(100 + number % 1_000, 2)));
            stock.add(new Inventory(sku, stockPerItem));
        }
        catalog.saveAll(items);
        inventory.saveAll(stock);
        System.out.printf(Locale.ROOT, "Seeded %,d catalog items with %,d units each.%n",
                catalogSize, stockPerItem);
    }
}
