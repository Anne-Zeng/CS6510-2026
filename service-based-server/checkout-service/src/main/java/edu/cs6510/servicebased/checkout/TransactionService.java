package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.shared.common.BusinessException;
import edu.cs6510.servicebased.shared.contract.*;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import edu.cs6510.servicebased.shared.persistence.CheckoutStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;
import static edu.cs6510.servicebased.shared.common.BusinessException.Kind.*;

/** 功能：拥有结账事务；热路径使用共享 JDBC 存储减少往返，服务之间仍只通过数据库协作。 */
@Service
@Transactional
public class TransactionService {
    // 功能：共享存储负责 SQL 和原子扫描日志，目录快照避免逐次查询价格。
    private final CheckoutStore checkout;
    private final CatalogCache catalog;
    private final int lowStockThreshold;

    // 功能：注入本地组件和低库存阈值；不创建任何服务间 HTTP/RPC 客户端。
    public TransactionService(CheckoutStore checkout, CatalogCache catalog,
                              @Value("${checkout.low-stock-threshold:50}") int lowStockThreshold) {
        if (lowStockThreshold < 0) throw new IllegalArgumentException("Negative low-stock threshold");
        this.checkout = checkout;
        this.catalog = catalog;
        this.lowStockThreshold = lowStockThreshold;
    }

    // 功能：一次 INSERT 创建 OPEN 交易，省去 JPA 的存在性查询；由 Spring 事务提交。
    public TransactionResponse startTransaction(StartTransactionRequest request) {
        requireText(request.stationId(), "stationId");
        String id = UUID.randomUUID().toString();
        String stationId = request.stationId().trim();
        Instant now = Instant.now();
        checkout.create(id, stationId, now);
        return new TransactionResponse(id, stationId, "OPEN", 0, new java.math.BigDecimal("0.00"), now);
    }

    // 功能：只读查询交易；不锁行，不加载购物篮实体图。
    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(String transactionId) { return checkout.find(transactionId); }

    // 功能：一条 SQL 完成购物篮及持久化 outbox，所有写入随本方法的数据库事务一起提交。
    public ScanResult scanItem(String transactionId, ScanItemRequest request) {
        requireText(request.sku(), "sku");
        var item = catalog.find(request.sku().trim()).orElseThrow(() ->
                new BusinessException(NOT_FOUND, "SKU_NOT_FOUND", "SKU not found"));
        // 功能：扫描不再等待全局序号锁；Analytics 会从已提交的 outbox 批量生成分析序号。
        return checkout.scan(transactionId, item);
    }

    // 功能：交易行锁防重复完成；集合式库存扣减、状态更新和收据使用同一个事务。
    @Transactional
    public Receipt completeTransaction(String transactionId) {
        var tx = checkout.lockOpen(transactionId);
        var lines = checkout.lines(transactionId);
        if (lines.isEmpty()) throw new BusinessException(CONFLICT, "EMPTY_TRANSACTION", "Transaction basket is empty");
        Instant completedAt = Instant.now();
        // 功能：合并库存与状态写入的一次往返；SQL 依赖库存扣减计数后才允许完成交易。
        var written = checkout.completeWithInventory(transactionId, lines.size(), lowStockThreshold, completedAt);
        // 功能：任意商品不足时抛异常，将同一 Spring 事务已扣减的其他商品也全部回滚。
        if (written == null || written.debitedLines() != lines.size()) {
            throw new BusinessException(CONFLICT, "INSUFFICIENT_STOCK", "Insufficient stock for one or more items");
        }
        // 功能：即使库存扣减成功，也必须确认状态实际更新；防止触发器等异常导致静默成功。
        if (written.completedTransactions() != 1) {
            throw new IllegalStateException("Checkout completion was not persisted");
        }
        return new Receipt(tx.transactionId(), tx.stationId(), tx.itemCount(), tx.runningTotal(),
                tx.startedAt(), completedAt, lines);
    }

    // 功能：业务层保留必填校验，也覆盖直接调用服务对象的场景。
    private static void requireText(String value, String name) {
        if (value == null || value.isBlank())
            throw new BusinessException(INVALID_REQUEST, "INVALID_REQUEST", name + " is required");
    }
}
