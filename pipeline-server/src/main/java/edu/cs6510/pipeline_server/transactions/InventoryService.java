package edu.cs6510.pipeline_server.transactions;

import edu.cs6510.pipeline_server.common.BusinessException;
import edu.cs6510.pipeline_server.contract.LowStockAlert;
import edu.cs6510.pipeline_server.contract.LowStockResponse;
import edu.cs6510.pipeline_server.persistence.InventoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static edu.cs6510.pipeline_server.common.BusinessException.Kind.*;

@Service
public class InventoryService {
    private final InventoryRepository inventory;
    private final int defaultThreshold;

    public InventoryService(InventoryRepository inventory,
                            @Value("${checkout.low-stock-threshold:50}") int defaultThreshold) {
        if (defaultThreshold < 0) throw new IllegalArgumentException("Negative low-stock threshold");
        this.inventory = inventory;
        this.defaultThreshold = defaultThreshold;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void decrementForCompletion(String sku, int quantity) {
        if (quantity <= 0) {
            throw new BusinessException(INVALID_REQUEST, "INVALID_QUANTITY", "Quantity must be positive");
        }
        if (inventory.decrementIfEnough(sku, quantity, defaultThreshold, Instant.now()) != 1) {
            throw new BusinessException(CONFLICT, "INSUFFICIENT_STOCK", "Insufficient stock for SKU: " + sku);
        }
    }

    @Transactional(readOnly = true)
    public LowStockResponse getLowStock(Integer requestedThreshold) {
        int threshold = requestedThreshold == null ? defaultThreshold : requestedThreshold;
        if (threshold < 0) {
            throw new BusinessException(INVALID_REQUEST, "INVALID_REQUEST", "threshold must be zero or greater");
        }
        Instant generatedAt = Instant.now();
        var alerts = inventory.findLowStock(threshold).stream().map(row ->
                new LowStockAlert(row.getSku(), row.getName(), row.getCurrentStock(), threshold,
                        row.getTriggeredAt() == null ? generatedAt : row.getTriggeredAt())).toList();
        return new LowStockResponse(threshold, generatedAt, alerts);
    }
}
