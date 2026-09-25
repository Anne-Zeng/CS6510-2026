package edu.cs6510.layered_server.api;

import edu.cs6510.layered_server.analytics.AnalyticsService;
import edu.cs6510.layered_server.contract.ScanItemRequest;
import edu.cs6510.layered_server.contract.ScanResult;
import edu.cs6510.layered_server.transactions.TransactionService;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.*;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.*;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises the real Spring proxy: removing @Transactional breaks these tests. */
class CheckoutFacadeTransactionTest {
    @Configuration
    @EnableTransactionManagement
    static class Config {
        @Bean TransactionService transactions() { return mock(TransactionService.class); }
        @Bean AnalyticsService analytics() { return mock(AnalyticsService.class); }
        @Bean TrackingTransactions transactionManager() { return new TrackingTransactions(); }
        @Bean CheckoutFacade checkout(TransactionService transactions, AnalyticsService analytics) {
            return new CheckoutFacade(transactions, analytics);
        }
    }

    static class TrackingTransactions extends AbstractPlatformTransactionManager {
        int commits;
        int rollbacks;
        static class Tx { boolean active; }
        private final ThreadLocal<Tx> current = new ThreadLocal<>();
        @Override protected Object doGetTransaction() {
            return current.get() == null ? new Tx() : current.get();
        }
        @Override protected boolean isExistingTransaction(Object tx) { return ((Tx) tx).active; }
        @Override protected void doBegin(Object tx, TransactionDefinition definition) {
            ((Tx) tx).active = true;
            current.set((Tx) tx);
        }
        @Override protected void doCleanupAfterCompletion(Object tx) { current.remove(); }
        @Override protected void doCommit(DefaultTransactionStatus status) { commits++; }
        @Override protected void doRollback(DefaultTransactionStatus status) { rollbacks++; }
    }

    @Test void bothLayersRunInsideTheScanTransaction() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            var transactions = context.getBean(TransactionService.class);
            var analytics = context.getBean(AnalyticsService.class);
            var manager = context.getBean(TrackingTransactions.class);
            var request = new ScanItemRequest("SKU-000001");
            var result = new ScanResult("tx", request.sku(), "Item", BigDecimal.ONE, 1, BigDecimal.ONE);
            when(transactions.scanItem("tx", request)).thenAnswer(call -> {
                assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
                return result;
            });
            doAnswer(call -> {
                assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
                return null;
            }).when(analytics).recordScan(request.sku());
            manager.commits = 0; // Mock stubbing may pass through the service proxy.
            assertEquals(result, context.getBean(CheckoutFacade.class).scanItem("tx", request));
            verify(analytics).recordScan(request.sku());
            assertEquals(1, manager.commits);
            assertEquals(0, manager.rollbacks);
        }
    }

    @Test void analyticsFailureRollsBackTheWholeScan() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            var request = new ScanItemRequest("SKU-000001");
            when(context.getBean(TransactionService.class).scanItem("tx", request))
                    .thenReturn(new ScanResult("tx", request.sku(), "Item", BigDecimal.ONE, 1, BigDecimal.ONE));
            doThrow(new IllegalStateException("event write failed"))
                    .when(context.getBean(AnalyticsService.class)).recordScan(request.sku());
            var manager = context.getBean(TrackingTransactions.class);
            manager.commits = 0;
            assertThrows(IllegalStateException.class,
                    () -> context.getBean(CheckoutFacade.class).scanItem("tx", request));
            assertEquals(0, manager.commits);
            assertEquals(1, manager.rollbacks);
        }
    }
}
