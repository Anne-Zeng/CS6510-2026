package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.shared.common.BusinessException;
import edu.cs6510.servicebased.shared.contract.LowStockAlert;
import edu.cs6510.servicebased.shared.contract.LowStockResponse;
import edu.cs6510.servicebased.shared.persistence.InventoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import static edu.cs6510.servicebased.shared.common.BusinessException.Kind.*;

/** 功能：结账服务内部的库存规则；与交易业务共用同一个数据库事务。 */
@Service
public class InventoryService {
    // 功能：保存库存持久化入口和默认低库存阈值。
    private final InventoryRepository inventory;
    private final int defaultThreshold;

    // 功能：从配置读取阈值；拒绝无意义的负阈值。
    public InventoryService(InventoryRepository inventory,
                            @Value("${checkout.low-stock-threshold:50}") int defaultThreshold) {
        if (defaultThreshold < 0) throw new IllegalArgumentException("Negative low-stock threshold");
        this.inventory = inventory;
        this.defaultThreshold = defaultThreshold;
    }

    // 功能：强制加入已有结账事务；条件 UPDATE 的影响行数决定库存是否足够。
    @Transactional(propagation = Propagation.MANDATORY)
    public void decrementForCompletion(String sku, int quantity) {
        if (quantity <= 0) throw new BusinessException(INVALID_REQUEST, "INVALID_QUANTITY", "Quantity must be positive");
        if (inventory.decrementIfEnough(sku, quantity, defaultThreshold, Instant.now()) != 1) {
            throw new BusinessException(CONFLICT, "INSUFFICIENT_STOCK", "Insufficient stock for SKU: " + sku);
        }
    }

    // 功能：查询低于阈值的商品；未持久化过触发时间时使用本次查询时间。
    @Transactional(readOnly = true)
    public LowStockResponse getLowStock(Integer requestedThreshold) {
        int threshold = requestedThreshold == null ? defaultThreshold : requestedThreshold;
        if (threshold < 0) throw new BusinessException(INVALID_REQUEST, "INVALID_REQUEST", "threshold must be zero or greater");
        Instant now = Instant.now();
        var alerts = inventory.findLowStock(threshold).stream().map(row -> new LowStockAlert(row.getSku(),
                row.getName(), row.getCurrentStock(), threshold,
                row.getTriggeredAt() == null ? now : row.getTriggeredAt())).toList();
        return new LowStockResponse(threshold, now, alerts);
    }
}
