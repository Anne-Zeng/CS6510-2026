package edu.cs6510.servicebased.checkout.persistence;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "CHECKOUT_TEST_DB_URL", matches = ".+")
class SaleCompletionPostgresTest {
    private final String schema = "sale_test_" + UUID.randomUUID().toString().replace("-", "");
    private final String url = System.getenv("CHECKOUT_TEST_DB_URL");
    private final String user = System.getenv("CHECKOUT_TEST_DB_USER");
    private final String password = System.getenv("CHECKOUT_TEST_DB_PASSWORD");
    private JdbcTemplate jdbc;
    private SagaStore store;
    private String transactionId;
    @BeforeEach void initialize() throws Exception {
        try (var connection = DriverManager.getConnection(url, user, password); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        var source = new DriverManagerDataSource(url, user, password);
        var properties = new Properties();
        properties.setProperty("currentSchema", schema);
        source.setConnectionProperties(properties);
        jdbc = new JdbcTemplate(source);
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        jdbc.execute(Files.readString(Path.of("database/006-sale-outbox.sql")));
        store = new SagaStore(jdbc);
        transactionId = UUID.randomUUID().toString();
        jdbc.update("""
            INSERT INTO checkout_transactions(transaction_id,station_id,status,item_count,running_total,started_at)
            VALUES (?,'test','OPEN',3,3,now())
            """, transactionId);
        jdbc.update("INSERT INTO transaction_items(transaction_id,sku,item_name,unit_price,quantity) VALUES (?,'A','Apple',1,3)", transactionId);
    }
    @AfterEach void cleanup() throws Exception {
        try (var connection = DriverManager.getConnection(url, user, password); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }
    @Test void finishWritesOneSaleWithQuantityAndRepeatedCompleteReturnsReceipt() {
        var work = store.begin(transactionId).work();
        assertNotNull(store.finish(work));
        assertNotNull(store.begin(transactionId).receipt());
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM sale_outbox", Long.class));
        assertEquals(3, jdbc.queryForObject("SELECT (payload->'items'->0->>'quantity')::integer FROM sale_outbox", Integer.class));
    }
    @Test void cancellationDoesNotPublishSale() {
        var work = store.begin(transactionId).work();
        assertNull(store.finish(store.decide(work, "CANCELLING")));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM sale_outbox", Long.class));
    }
    @Test void outboxFailureRollsBackCompletedState() {
        var work = store.begin(transactionId).work();
        jdbc.execute("ALTER TABLE sale_outbox ADD CONSTRAINT reject_sale CHECK(false)");
        assertThrows(RuntimeException.class, () -> store.finish(work));
        assertEquals("CHECKOUT_PENDING", jdbc.queryForObject("SELECT status FROM checkout_transactions WHERE transaction_id=?", String.class, transactionId));
    }
}
