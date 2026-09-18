package edu.cs6510.monolith_server.inventory;

import edu.cs6510.monolith_server.catalog.CatalogItem;
import edu.cs6510.monolith_server.catalog.CatalogItemRepository;
import edu.cs6510.monolith_server.inventory.api.LowStockAlert;
import edu.cs6510.monolith_server.inventory.api.LowStockResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;

@Service
public class InventoryService {

    public static final int DEFAULT_LOW_STOCK_THRESHOLD = 50;

    private final InventoryRepository inventoryRepository;
    private final CatalogItemRepository catalogItemRepository;

    public InventoryService(
            InventoryRepository inventoryRepository,
            CatalogItemRepository catalogItemRepository
    ) {
        this.inventoryRepository = inventoryRepository;
        this.catalogItemRepository = catalogItemRepository;
    }

    @Transactional
    public void decrementForCompletion(String sku, int quantity) {
        int rowsUpdated = inventoryRepository.decrementIfEnough(sku, quantity);

        if (rowsUpdated == 0) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Insufficient stock for SKU: " + sku
            );
        }

        Inventory inventory = inventoryRepository.findById(sku)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Inventory SKU not found"
                ));

        if (inventory.getCurrentStock() <= DEFAULT_LOW_STOCK_THRESHOLD) {
            inventory.markLowStock(Instant.now());
            inventoryRepository.save(inventory);
        }
    }

    @Transactional(readOnly = true)
    public LowStockResponse getLowStock(Integer requestedThreshold) {
        int threshold = requestedThreshold == null
                ? DEFAULT_LOW_STOCK_THRESHOLD
                : requestedThreshold;

        if (threshold < 0) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "threshold must be zero or greater"
            );
        }

        List<LowStockAlert> alerts = inventoryRepository
                .findAllByCurrentStockLessThanEqualOrderByCurrentStockAsc(threshold)
                .stream()
                .map(inventory -> {
                    CatalogItem item = catalogItemRepository
                            .findById(inventory.getSku())
                            .orElseThrow(() -> new ResponseStatusException(
                                    HttpStatus.NOT_FOUND,
                                    "Catalog SKU not found"
                            ));

                    Instant triggeredAt = inventory.getLowStockTriggeredAt() == null
                            ? Instant.now()
                            : inventory.getLowStockTriggeredAt();

                    return new LowStockAlert(
                            inventory.getSku(),
                            item.getName(),
                            inventory.getCurrentStock(),
                            threshold,
                            triggeredAt
                    );
                })
                .toList();

        return new LowStockResponse(
                threshold,
                Instant.now(),
                alerts
        );
    }
}