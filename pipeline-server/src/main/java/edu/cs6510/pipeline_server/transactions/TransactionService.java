package edu.cs6510.pipeline_server.transactions;

import edu.cs6510.pipeline_server.common.BusinessException;
import edu.cs6510.pipeline_server.contract.*;
import edu.cs6510.pipeline_server.persistence.*;
import edu.cs6510.pipeline_server.persistence.entity.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static edu.cs6510.pipeline_server.common.BusinessException.Kind.*;

/** Checkout rules only. No dependency on analytics.
 * Scan operations join the API coordinator's transaction; other operations own theirs.
 */
@Service
@Transactional
public class TransactionService {
    private final CheckoutTransactionRepository transactions;
    private final TransactionItemRepository baskets;
    private final CatalogCache catalog;
    private final InventoryService inventory;

    public TransactionService(CheckoutTransactionRepository transactions,
                              TransactionItemRepository baskets,
                              CatalogCache catalog,
                              InventoryService inventory) {
        this.transactions = transactions;
        this.baskets = baskets;
        this.catalog = catalog;
        this.inventory = inventory;
    }

    public TransactionResponse startTransaction(StartTransactionRequest request) {
        requireText(request.stationId(), "stationId");
        CheckoutTransaction tx = new CheckoutTransaction(
                UUID.randomUUID().toString(), request.stationId().trim(), Instant.now());
        transactions.save(tx);
        return response(tx);
    }

    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(String transactionId) {
        return response(findTransaction(transactionId));
    }

    public ScanResult scanItem(String transactionId, ScanItemRequest request) {
        CheckoutTransaction tx = findOpenTransaction(transactionId);
        requireText(request.sku(), "sku");
        // Cached lookup: the catalog is immutable after seeding, so this costs no query.
        CatalogItem item = catalog.find(request.sku().trim()).orElseThrow(() ->
                new BusinessException(NOT_FOUND, "SKU_NOT_FOUND", "SKU not found"));
        TransactionItem line = baskets
                .findByTransactionTransactionIdAndSku(transactionId, item.getSku()).orElse(null);
        if (line == null) {
            line = new TransactionItem(tx, item.getSku(), item.getName(), item.getPrice());
            baskets.save(line);
        } else {
            line.addOne();
        }
        // Tracked JPA entities are flushed when the enclosing API transaction commits.
        tx.addScannedItem(line.getUnitPrice());
        return new ScanResult(tx.getTransactionId(), line.getSku(), line.getName(),
                line.getUnitPrice(), tx.getItemCount(), tx.getRunningTotal());
    }

    public Receipt completeTransaction(String transactionId) {
        CheckoutTransaction tx = findOpenTransaction(transactionId);
        List<TransactionItem> lines = baskets.findAllByTransactionTransactionId(transactionId)
                .stream().sorted(Comparator.comparing(TransactionItem::getSku)).toList();
        if (lines.isEmpty()) {
            throw new BusinessException(CONFLICT, "EMPTY_TRANSACTION", "Transaction basket is empty");
        }
        for (TransactionItem line : lines) {
            inventory.decrementForCompletion(line.getSku(), line.getQuantity());
        }
        // Status and every SKU debit commit together; exceptions roll everything back.
        Instant completedAt = Instant.now();
        tx.complete(completedAt);
        return new Receipt(tx.getTransactionId(), tx.getStationId(), tx.getItemCount(),
                tx.getRunningTotal(), tx.getStartedAt(), completedAt,
                lines.stream().map(line -> new ReceiptLine(line.getSku(), line.getName(),
                        line.getUnitPrice(), line.getQuantity())).toList());
    }

    private CheckoutTransaction findTransaction(String id) {
        return transactions.findById(id).orElseThrow(() ->
                new BusinessException(NOT_FOUND, "TRANSACTION_NOT_FOUND", "Transaction not found"));
    }

    private CheckoutTransaction findOpenTransaction(String id) {
        CheckoutTransaction tx = findTransaction(id);
        if (tx.getStatus() != TransactionStatus.OPEN) {
            throw new BusinessException(CONFLICT, "TRANSACTION_NOT_OPEN", "Transaction is not open");
        }
        return tx;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new BusinessException(INVALID_REQUEST, "INVALID_REQUEST", name + " is required");
        }
    }

    private static TransactionResponse response(CheckoutTransaction tx) {
        return new TransactionResponse(tx.getTransactionId(), tx.getStationId(),
                tx.getStatus().name(), tx.getItemCount(), tx.getRunningTotal(), tx.getStartedAt());
    }
}
