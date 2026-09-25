package edu.cs6510.layered_server.api;

import edu.cs6510.layered_server.analytics.AnalyticsService;
import edu.cs6510.layered_server.contract.*;
import edu.cs6510.layered_server.transactions.TransactionService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * API-layer use-case coordinator, and the only place where the two middle
 * layers meet. They are siblings: {@link TransactionService} and
 * {@link AnalyticsService} have no reference to each other, and this class is
 * what turns one HTTP request into a call on each of them.
 *
 * <p>The scan use case owns one transaction so the basket update and its
 * analytics event either both commit or both roll back. Window publication
 * runs separately on the analytics scheduler.
 */
@Service
public class CheckoutFacade {
    private final TransactionService transactions;
    private final AnalyticsService analytics;

    public CheckoutFacade(TransactionService transactions, AnalyticsService analytics) {
        this.transactions = transactions;
        this.analytics = analytics;
    }

    public TransactionResponse startTransaction(StartTransactionRequest request) {
        return transactions.startTransaction(request);
    }

    public TransactionResponse getTransaction(String transactionId) {
        return transactions.getTransaction(transactionId);
    }

    /**
     * Record the basket update and its analytics event atomically.
     */
    //the API coordinates both transaction processing and analytics,
    // while those two responsibilities remain separate.
    @Transactional
    public ScanResult scanItem(String transactionId, ScanItemRequest request) {
        //First, the API coordinator asks the transaction layer to add the scanned item to the transaction.
        ScanResult result = transactions.scanItem(transactionId, request);
        //Then, it tells the analytics layer which item was scanned. sku() returns the item’s identifier.
        analytics.recordScan(result.sku());
        //Finally, it returns the scan result to the controller, which responds to the client.
        return result;
    }

    public Receipt completeTransaction(String transactionId) {
        return transactions.completeTransaction(transactionId);
    }
}
