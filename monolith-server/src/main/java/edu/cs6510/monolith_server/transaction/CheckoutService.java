package edu.cs6510.monolith_server.transaction;

import edu.cs6510.monolith_server.catalog.CatalogItem;
import edu.cs6510.monolith_server.catalog.CatalogItemRepository;
import edu.cs6510.monolith_server.inventory.InventoryRepository;
import edu.cs6510.monolith_server.inventory.InventoryService;
import edu.cs6510.monolith_server.transaction.api.Receipt;
import edu.cs6510.monolith_server.transaction.api.ReceiptLine;
import edu.cs6510.monolith_server.transaction.api.ScanItemRequest;
import edu.cs6510.monolith_server.transaction.api.ScanResult;
import edu.cs6510.monolith_server.transaction.api.StartTransactionRequest;
import edu.cs6510.monolith_server.transaction.api.TransactionResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import edu.cs6510.monolith_server.analytics.AnalyticsService;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

// Checkout logic that uses the repository at payment completion.
// The CheckoutService class is a service component in the application that handles the business logic related to checkout transactions. 
// It is responsible for starting new checkout transactions and managing their lifecycle. 
// The class is annotated with @Service, indicating that it is a Spring-managed service component, and it uses the CheckoutTransactionRepository to 
// interact with the database for persisting and retrieving CheckoutTransaction entities. 
// The startTransaction() method is annotated with @Transactional, ensuring that the entire operation of starting a transaction is executed within a single database transaction, providing consistency and rollback capabilities in case of errors.
// The startTransaction() method takes a StartTransactionRequest object as input, which contains the stationId where the transaction is being initiated.
// It creates a new CheckoutTransaction entity with a unique transactionId, the provided stationId, and the current timestamp as the startedAt value. The new transaction is then saved to the database using the transactionRepository, and a TransactionResponse object is returned to the client, containing the details of the newly created transaction.
// The CheckoutService class encapsulates the core functionality of starting a checkout transaction, ensuring that the necessary data is captured and persisted correctly, while also providing a clear and structured response to the client.
@Service
public class CheckoutService {

    private final CheckoutTransactionRepository transactionRepository;
    private final TransactionItemRepository transactionItemRepository;
    private final CatalogItemRepository catalogItemRepository;
    private final InventoryService inventoryService;
    private final AnalyticsService analyticsService;

    public CheckoutService(
            CheckoutTransactionRepository transactionRepository,
            TransactionItemRepository transactionItemRepository,
            CatalogItemRepository catalogItemRepository,
            InventoryService inventoryService,
            AnalyticsService analyticsService
    ) {
        this.transactionRepository = transactionRepository;
        this.transactionItemRepository = transactionItemRepository;
        this.catalogItemRepository = catalogItemRepository;
        this.inventoryService = inventoryService;
        this.analyticsService = analyticsService;
    }

    @Transactional
    public TransactionResponse startTransaction(StartTransactionRequest request) {
        CheckoutTransaction transaction = new CheckoutTransaction(
                UUID.randomUUID().toString(),
                request.stationId().trim(),
                Instant.now()
        );

        transactionRepository.save(transaction);
        return TransactionResponse.from(transaction);
    }

    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(String transactionId) {
        CheckoutTransaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Transaction not found"
                ));

        return TransactionResponse.from(transaction);
    }

    @Transactional
    public ScanResult scanItem(String transactionId, ScanItemRequest request) {
        CheckoutTransaction transaction = findOpenTransaction(transactionId);//transactionRepository.findById(transactionId)
        String sku = request.sku().trim();
        //         .orElseThrow(() -> new ResponseStatusException(
        //                 HttpStatus.NOT_FOUND,
        //                 "SKU not found"
        //         ));

        // if (transaction.getStatus() != TransactionStatus.OPEN) {
        //     throw new ResponseStatusException(
        //             HttpStatus.CONFLICT,
        //             "Transaction is not open"
        //     );
        // }
        CatalogItem catalogItem = catalogItemRepository.findById(sku)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "SKU not found"
                ));

        TransactionItem transactionItem = transactionItemRepository
                .findByTransactionTransactionIdAndSku(transactionId, sku)
                .orElse(null);

        if (transactionItem == null) {
            transactionItem = new TransactionItem(
                    transaction,
                    catalogItem.getSku(),
                    catalogItem.getName(),
                    catalogItem.getPrice()
            );
        } else {
            transactionItem.addOne();
        }

        transactionItemRepository.save(transactionItem);

        // Inventory is unchanged at scan time. This changes only the basket total, never inventory.
        transaction.addScannedItem(catalogItem.getPrice());
        transactionRepository.save(transaction);
        analyticsService.recordScan(catalogItem.getSku());

        return new ScanResult(
                transaction.getTransactionId(),
                catalogItem.getSku(),
                catalogItem.getName(),
                catalogItem.getPrice(),
                transaction.getItemCount(),
                transaction.getRunningTotal()
        );
    }

    @Transactional
    public Receipt completeTransaction(String transactionId) {
        CheckoutTransaction transaction = findOpenTransaction(transactionId);

        List<TransactionItem> transactionItems =
                transactionItemRepository.findAllByTransactionTransactionId(transactionId);

        if (transactionItems.isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Transaction basket is empty"
            );
        }

        for (TransactionItem item : transactionItems) {
            inventoryService.decrementForCompletion(
                    item.getSku(),
                    item.getQuantity()
            );
            // rowsUpdated == 1: one matching SKU had enough stock, so its stock was reduced.
            // rowsUpdated == 0: no row matched—either the SKU is missing or its stock was too low.
            // int rowsUpdated = inventoryRepository.decrementIfEnough(
            //         item.getSku(),
            //         item.getQuantity()
            // );
            // //Rejects the completion when the result is 0:
            // if (rowsUpdated == 0) {
            //     // Because this method is transactional, prior stock updates roll back too.
            //     throw new ResponseStatusException(
            //             HttpStatus.CONFLICT,
            //             "Insufficient stock for SKU: " + item.getSku()
            //     );
            // }
        }

        Instant completedAt = Instant.now();
        transaction.complete(completedAt);
        transactionRepository.save(transaction);

        List<ReceiptLine> receiptLines = transactionItems.stream()
                .map(item -> new ReceiptLine(
                        item.getSku(),
                        item.getName(),
                        item.getUnitPrice(),
                        item.getQuantity()
                ))
                .toList();

        return new Receipt(
                transaction.getTransactionId(),
                transaction.getStationId(),
                transaction.getItemCount(),
                transaction.getRunningTotal(),
                transaction.getStartedAt(),
                completedAt,
                receiptLines
        );
    }



    private CheckoutTransaction findOpenTransaction(String transactionId) {
        CheckoutTransaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Transaction not found"
                ));

        if (transaction.getStatus() != TransactionStatus.OPEN) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Transaction is not open"
            );
        }
        return transaction;
    }
}