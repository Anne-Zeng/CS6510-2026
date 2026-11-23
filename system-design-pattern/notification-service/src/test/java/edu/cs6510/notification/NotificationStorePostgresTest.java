package edu.cs6510.notification;

import edu.cs6510.events.Json;
import edu.cs6510.events.SaleCompleted;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "NOTIFICATION_TEST_DB_URL", matches = ".+")
class NotificationStorePostgresTest {
    private final String schema = "notification_test_" + UUID.randomUUID().toString().replace("-", "");
    private final String url = System.getenv("NOTIFICATION_TEST_DB_URL");
    private final String user = System.getenv("NOTIFICATION_TEST_DB_USER");
    private final String password = System.getenv("NOTIFICATION_TEST_DB_PASSWORD");
    private static final String TOKEN = "test-credential-at-least-32-characters";
    private JdbcTemplate jdbc;
    private NotificationStore store;
    private TransactionTemplate tx;
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
        tx = new TransactionTemplate(new DataSourceTransactionManager(source));
        store = new NotificationStore(jdbc, 100, 2000);
        org.springframework.test.util.ReflectionTestUtils.setField(store, "dispatchCustomPlan", true);
    }
    @AfterEach void cleanup() throws Exception {
        try {
            assertMetricsMatchTables();
        } finally {
            try (var connection = DriverManager.getConnection(url, user, password); var statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        }
    }
    private void assertMetricsMatchTables() {
        assertEquals(jdbc.queryForMap("""
            SELECT (SELECT count(*) FROM subscribers) AS subscribers,
                   (SELECT count(*) FROM sale_inbox) AS sales,
                   count(*) AS notifications,
                   count(*) FILTER(WHERE acknowledged_at IS NULL) AS pending,
                   COALESCE(sum(attempts),0) AS send_attempts FROM notification_deliveries
            """), store.metrics());
    }
    @Test void sharedPayloadIsStoredOnceAndUnchangedByDispatch() {
        for (int subscriber = 0; subscriber < 100; subscriber++) subscribe("user-" + subscriber, "A", "B");
        accept(Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now().plusSeconds(1), List.of(new SaleCompleted.Item("A", 3), new SaleCompleted.Item("B", 2)))));
        assertEquals(2L, jdbc.queryForObject("SELECT count(*) FROM notification_sale_items", Long.class));
        assertEquals(200L, store.metrics().get("notifications"));
        var before = jdbc.queryForList("SELECT item_id,xmin::text AS version,quantity FROM notification_sale_items ORDER BY item_id");
        var identifiers = store.pending("user-0").stream().map(Protocol.Delivery::notificationId).toList();
        tx.executeWithoutResult(status -> store.dispatchBatch("user-0", identifiers, identifiers));
        assertEquals(before, jdbc.queryForList("SELECT item_id,xmin::text AS version,quantity FROM notification_sale_items ORDER BY item_id"));
        assertEquals(198L, store.metrics().get("pending"));
        var sizes = jdbc.queryForMap("""
                SELECT avg(pg_column_size(delivery)) AS compact,
                       avg(pg_column_size(ROW(delivery.notification_id,item.event_id,delivery.subscriber_id,
                           item.transaction_id,item.sku,item.quantity,item.completed_at,delivery.acknowledged_at,
                           delivery.next_attempt_at,delivery.attempts))) AS legacy
                FROM notification_deliveries delivery JOIN notification_sale_items item USING(item_id)
                """);
        assertTrue(((Number) sizes.get("compact")).doubleValue() < ((Number) sizes.get("legacy")).doubleValue());
    }
    @Test void cachedPayloadPreservesOwnershipAcknowledgementsAndWireContent() {
        subscribe("alice", "A", "B");
        subscribe("bob", "A");
        accept(event("A", 3, Instant.now().plusSeconds(1)));
        var original = store.pending("alice");
        store.configurePayloadCache(16);
        assertEquals(original, store.pending("alice"));
        assertEquals(original, store.pending("alice"));
        assertTrue(store.payloadCacheMetrics().get("hits") > 0);
        assertTrue(store.pending("unknown").isEmpty());
        var bob = store.pending("bob").getFirst();
        assertEquals("bob", bob.subscriberId());
        assertNotEquals(original.getFirst().notificationId(), bob.notificationId());
        assertThrows(IllegalArgumentException.class, () -> tx.executeWithoutResult(status ->
                store.acknowledge("bob", original.getFirst().notificationId())));
        tx.executeWithoutResult(status -> store.acknowledge("alice", original.getFirst().notificationId()));
        assertTrue(store.pending("alice").isEmpty());
        assertEquals(List.of(bob), store.pending("bob"));
        assertThrows(org.springframework.dao.DataAccessException.class, () ->
                jdbc.update("UPDATE notification_sale_items SET quantity=quantity+1"));
        var restarted = new NotificationStore(jdbc, 100, 2000);
        restarted.configurePayloadCache(16);
        assertEquals(List.of(bob), restarted.pending("bob"));
        assertTrue(restarted.pending("alice").isEmpty());
    }
    @Test void smallCacheDoesNotDropItemsFromLargerPendingBatch() {
        subscribe("alice", "A", "B");
        accept(event("A", 1, Instant.now().plusSeconds(1)), event("B", 2, Instant.now().plusSeconds(1)));
        var original = store.pending("alice");
        store.configurePayloadCache(1);
        assertEquals(original, store.pending("alice"));
        assertEquals(original, store.pending("alice"));
        assertEquals(1L, store.payloadCacheMetrics().get("size"));
    }
    private void restoreLegacyLayout() {
        jdbc.execute("""
                ALTER TABLE notification_deliveries ADD COLUMN event_id varchar(36),
                 ADD COLUMN transaction_id varchar(36), ADD COLUMN sku varchar(32),
                 ADD COLUMN quantity integer, ADD COLUMN completed_at timestamptz;
                UPDATE notification_deliveries delivery SET event_id=item.event_id,transaction_id=item.transaction_id,
                 sku=item.sku,quantity=item.quantity,completed_at=item.completed_at
                FROM notification_sale_items item WHERE item.item_id=delivery.item_id;
                ALTER TABLE notification_deliveries DROP COLUMN item_id;
                DROP TABLE notification_sale_items;
                """);
    }
    @Test void compactMigrationPreservesPayloadIdsAcknowledgementsAndRetryState() throws Exception {
        subscribe("alice", "A");
        subscribe("bob", "A");
        accept(event("A", 3, Instant.now().plusSeconds(1)));
        String alice = store.pending("alice").getFirst().notificationId();
        String bob = store.pending("bob").getFirst().notificationId();
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(alice), List.of()));
        tx.executeWithoutResult(status -> store.acknowledgeBatch("bob", List.of(bob)));
        var states = jdbc.queryForList("SELECT notification_id,acknowledged_at,next_attempt_at,attempts FROM notification_deliveries ORDER BY notification_id");
        var metrics = store.metrics();
        restoreLegacyLayout();
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        assertEquals(states, jdbc.queryForList("SELECT notification_id,acknowledged_at,next_attempt_at,attempts FROM notification_deliveries ORDER BY notification_id"));
        assertEquals(metrics, store.metrics());
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM notification_sale_items", Long.class));
        jdbc.update("UPDATE notification_deliveries SET next_attempt_at=now()-interval '1 second' WHERE notification_id=?::uuid", alice);
        var restored = store.pending("alice").getFirst();
        assertEquals(alice, restored.notificationId());
        assertEquals("A", restored.sku());
        assertEquals(3, restored.quantity());
        assertTrue(store.pending("bob").isEmpty());
    }
    @Test void corruptLegacyPayloadAbortsMigrationWithoutDroppingOriginalColumns() throws Exception {
        subscribe("alice", "A");
        accept(event("A", 3, Instant.now().plusSeconds(1)));
        restoreLegacyLayout();
        jdbc.update("UPDATE notification_deliveries SET quantity=999");
        String migration = Files.readString(Path.of("database/schema.sql"));
        assertThrows(RuntimeException.class, () -> jdbc.execute(migration));
        assertEquals(999, jdbc.queryForObject("SELECT quantity FROM notification_deliveries", Integer.class));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM pg_attribute WHERE attrelid='notification_deliveries'::regclass AND attname='item_id' AND NOT attisdropped", Long.class));
        assertNull(jdbc.queryForObject("SELECT to_regclass('notification_sale_items')::text", String.class));
    }
    @Test void orderedIdentifiersHaveUuid7LayoutAndUniqueRandomSuffixes() {
        long before = System.currentTimeMillis();
        var identifiers = jdbc.queryForList("SELECT notification_ordered_uuid() FROM generate_series(1,1000)", String.class);
        long after = System.currentTimeMillis();
        assertEquals(1000, new java.util.HashSet<>(identifiers).size());
        for (String text : identifiers) {
            UUID identifier = UUID.fromString(text);
            assertEquals(7, identifier.version());
            assertEquals(2, identifier.variant());
            long timestamp = identifier.getMostSignificantBits() >>> 16;
            assertTrue(timestamp >= before && timestamp <= after);
        }
    }
    @Test void orderedIdMigrationPreservesLegacyIdsAndAcknowledgements() throws Exception {
        jdbc.execute("ALTER TABLE notification_deliveries ALTER COLUMN notification_id SET DEFAULT gen_random_uuid()");
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        String legacy = store.pending("alice").getFirst().notificationId();
        assertEquals(4, UUID.fromString(legacy).version());
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        accept(event("A", 2, Instant.now().plusSeconds(1)));
        var deliveries = store.pending("alice");
        assertEquals(2, deliveries.size());
        assertTrue(deliveries.stream().anyMatch(delivery -> delivery.notificationId().equals(legacy)));
        assertTrue(deliveries.stream().anyMatch(delivery -> UUID.fromString(delivery.notificationId()).version() == 7));
        tx.executeWithoutResult(status -> store.acknowledgeBatch("alice", deliveries.stream().map(Protocol.Delivery::notificationId).toList()));
        assertTrue(store.pending("alice").isEmpty());
    }
    @Test void mixedReplayAndNewSalesUseOneFanoutStatement() {
        subscribe("alice", "A", "B");
        subscribe("bob", "B");
        String old = event("A", 1, Instant.now().plusSeconds(1));
        String fresh = event("B", 2, Instant.now().plusSeconds(1));
        accept(old);
        jdbc.execute("""
                CREATE TABLE fanout_statements(total integer);
                CREATE FUNCTION audit_fanout() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN INSERT INTO fanout_statements VALUES ((SELECT count(*) FROM inserted_rows)); RETURN NULL; END $$;
                CREATE TRIGGER audit_fanout AFTER INSERT ON notification_deliveries
                REFERENCING NEW TABLE AS inserted_rows FOR EACH STATEMENT EXECUTE FUNCTION audit_fanout();
                """);
        accept(old, fresh, fresh, event("A", 3, Instant.now().plusSeconds(1)));
        assertEquals(List.of(3), jdbc.queryForList("SELECT total FROM fanout_statements", Integer.class));
        assertEquals(3L, store.metrics().get("sales"));
        assertEquals(4L, store.metrics().get("notifications"));
        assertEquals(2, store.pending("bob").getFirst().quantity());
    }
    @Test void fanoutDatabaseFailureRollsBackEntireBulkInbox() {
        subscribe("alice", "A", "B");
        jdbc.execute("ALTER TABLE notification_deliveries ADD CONSTRAINT reject_test_delivery CHECK(false)");
        assertThrows(RuntimeException.class, () -> accept(event("A", 1, Instant.now().plusSeconds(1)),
                event("B", 2, Instant.now().plusSeconds(1))));
        assertEquals(0L, store.metrics().get("sales"));
        assertEquals(0L, store.metrics().get("notifications"));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM notification_sale_items", Long.class));
    }
    @Test void conflictWithinBatchRollsBackOtherNewSales() {
        subscribe("alice", "A");
        String first = event("A", 1, Instant.now().plusSeconds(1));
        var original = Json.read(first, SaleCompleted.class);
        String conflict = Json.write(new SaleCompleted(1, original.eventId(), original.transactionId(),
                original.completedAt(), List.of(new SaleCompleted.Item("A", 2))));
        assertThrows(IllegalArgumentException.class, () -> accept(first, conflict,
                event("A", 3, Instant.now().plusSeconds(1))));
        String otherEvent = Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), original.transactionId(),
                original.completedAt(), original.items()));
        assertThrows(IllegalArgumentException.class, () -> accept(first, otherEvent));
        assertEquals(0L, store.metrics().get("sales"));
        assertEquals(0L, store.metrics().get("notifications"));
    }
    @Test void concurrentReplayFansOutOnceWithoutDeliveryUniqueIndex() throws Exception {
        subscribe("alice", "A", "B");
        subscribe("bob", "B");
        String sale = Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now().plusSeconds(1), List.of(new SaleCompleted.Item("A", 3), new SaleCompleted.Item("B", 2))));
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int worker = 0; worker < 2; worker++) {
                tasks.add(executor.submit(() -> {
                    assertTrue(start.await(10, java.util.concurrent.TimeUnit.SECONDS));
                    accept(sale, sale);
                    return null;
                }));
            }
            start.countDown();
            for (var task : tasks) task.get(20, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertEquals(1L, store.metrics().get("sales"));
        assertEquals(3L, store.metrics().get("notifications"));
        assertEquals(2, store.pending("alice").size());
        assertEquals(1, store.pending("bob").size());
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM pg_constraint WHERE conrelid='notification_deliveries'::regclass AND contype='u'", Long.class));
    }
    @Test void conflictingReplayAndDuplicateSkuCannotCreateDeliveries() {
        subscribe("alice", "A");
        String sale = event("A", 1, Instant.now().plusSeconds(1));
        accept(sale);
        var original = Json.read(sale, SaleCompleted.class);
        String conflict = Json.write(new SaleCompleted(1, original.eventId(), original.transactionId(),
                original.completedAt(), List.of(new SaleCompleted.Item("A", 2))));
        assertThrows(IllegalArgumentException.class, () -> accept(conflict));
        assertThrows(IllegalArgumentException.class, () -> new SaleCompleted(1, UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), Instant.now(), List.of(new SaleCompleted.Item("A", 1), new SaleCompleted.Item("A", 2))));
        assertEquals(1L, store.metrics().get("notifications"));
        assertEquals(1, store.pending("alice").getFirst().quantity());
    }
    @Test void metricsRollBackAndReplayDoesNotDoubleCount() {
        subscribe("alice", "A");
        String sale = event("A", 1, Instant.now().plusSeconds(1));
        accept(sale);
        var before = store.metrics();
        accept(sale);
        assertEquals(before, store.metrics());
        assertThrows(RuntimeException.class, () -> accept(event("A", 2, Instant.now().plusSeconds(1)), "{}"));
        assertEquals(before, store.metrics());
        var id = store.pending("alice").getFirst().notificationId();
        assertThrows(IllegalArgumentException.class, () -> tx.executeWithoutResult(status ->
                store.dispatchBatch("alice", List.of(id), List.of(id, UUID.randomUUID().toString()))));
        assertEquals(before, store.metrics());
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(id), List.of(id)));
        assertEquals(0L, store.metrics().get("pending"));
        assertEquals(1L, store.metrics().get("send_attempts"));
    }
    @Test void metricsFollowDeleteAndTruncate() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var id = store.pending("alice").getFirst().notificationId();
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(id), List.of(id)));
        jdbc.update("DELETE FROM notification_deliveries");
        assertMetricsMatchTables();
        accept(event("A", 2, Instant.now().plusSeconds(1)));
        jdbc.execute("TRUNCATE subscribers,sale_inbox CASCADE");
        assertEquals(0L, store.metrics().get("notifications"));
        assertEquals(0L, store.metrics().get("send_attempts"));
    }
    @Test void metricsBootstrapExistingRowsAndSchemaReinstallIsIdempotent() throws Exception {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var id = store.pending("alice").getFirst().notificationId();
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(id), List.of(id)));
        var before = store.metrics();
        jdbc.execute("DROP TABLE notification_metric_totals");
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        assertEquals(before, store.metrics());
        jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        assertEquals(before, store.metrics());
    }
    private void subscribe(String id, String... skus) {
        tx.executeWithoutResult(status -> store.subscribe(id, TOKEN, List.of(skus)));
    }
    private String event(String sku, int quantity, Instant completedAt) {
        return Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                completedAt, List.of(new SaleCompleted.Item(sku, quantity))));
    }
    private void accept(String... bodies) {
        tx.executeWithoutResult(status -> store.accept(List.of(bodies)));
    }
    @Test void routesDeduplicatesAndAcknowledgesOnlyOwner() {
        subscribe("alice", "A", "B");
        subscribe("bob", "B");
        String sale = event("A", 3, Instant.now().plusSeconds(1));
        accept(sale);
        accept(sale);
        var pending = store.pending("alice");
        assertEquals(1, pending.size());
        assertEquals(3, pending.getFirst().quantity());
        assertTrue(store.pending("bob").isEmpty());
        assertThrows(IllegalArgumentException.class, () -> store.acknowledge("bob", pending.getFirst().notificationId()));
        store.acknowledge("alice", pending.getFirst().notificationId());
        store.acknowledge("alice", pending.getFirst().notificationId());
        assertTrue(store.pending("alice").isEmpty());
    }
    @Test void supports100AndRejects101WithoutBlockingReconnect() {
        for (int index = 0; index < 100; index++) subscribe("user-" + index, "A");
        assertThrows(IllegalArgumentException.class, () -> subscribe("overflow", "A"));
        subscribe("user-0", "A");
        assertThrows(IllegalArgumentException.class, () -> store.subscribe("user-0", "wrong-credential-at-least-32-characters", List.of("A")));
        assertThrows(IllegalArgumentException.class, () -> subscribe("user-0", "B"));
    }
    @Test void oldSalesAreNotDeliveredAndInvalidBatchRollsBack() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().minusSeconds(60)));
        assertTrue(store.pending("alice").isEmpty());
        assertThrows(RuntimeException.class, () -> accept(event("A", 1, Instant.now().plusSeconds(1)), "{}"));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM sale_inbox", Long.class));
    }
    @Test void pendingDeliverySurvivesStoreRecreation() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var restored = new NotificationStore(jdbc, 100, 2000);
        assertEquals(1, restored.pending("alice").size());
    }
    @Test void fansOutMultipleItemsAndRollsBackMixedOwnerAcknowledgements() {
        subscribe("alice", "A", "B");
        subscribe("bob", "B");
        var event = new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now().plusSeconds(1), List.of(new SaleCompleted.Item("A", 3), new SaleCompleted.Item("B", 2)));
        accept(Json.write(event));
        var alice = store.pending("alice");
        var bob = store.pending("bob");
        assertEquals(2, alice.size());
        assertEquals(1, bob.size());
        assertThrows(IllegalArgumentException.class, () -> tx.executeWithoutResult(status ->
                store.acknowledgeBatch("alice", List.of(alice.getFirst().notificationId(), bob.getFirst().notificationId()))));
        assertEquals(2, store.pending("alice").size());
        tx.executeWithoutResult(status -> store.acknowledgeBatch("alice",
                alice.stream().map(Protocol.Delivery::notificationId).toList()));
        assertTrue(store.pending("alice").isEmpty());
        assertEquals(1, store.pending("bob").size());
    }
    @Test void dispatchCycleCommitsAccountingAndAckBeforeFetchingNextBatch() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)), event("A", 2, Instant.now().plusSeconds(1)));
        jdbc.execute("""
            CREATE TABLE delivery_write_audit(notification_id text);
            CREATE FUNCTION audit_delivery_write() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO delivery_write_audit VALUES(NEW.notification_id); RETURN NEW; END $$;
            CREATE TRIGGER delivery_write_audit AFTER UPDATE ON notification_deliveries
            FOR EACH ROW EXECUTE FUNCTION audit_delivery_write();
            """);
        var identifiers = store.pending("alice").stream().map(Protocol.Delivery::notificationId).toList();
        var next = tx.execute(status -> store.dispatchBatch("alice", identifiers, List.of(identifiers.getFirst())));
        assertTrue(next.isEmpty());
        assertEquals(2L, jdbc.queryForObject("SELECT sum(attempts) FROM notification_deliveries", Long.class));
        assertEquals(2L, jdbc.queryForObject("SELECT count(*) FROM delivery_write_audit", Long.class));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM notification_deliveries WHERE acknowledged_at IS NOT NULL", Long.class));
        jdbc.update("UPDATE notification_deliveries SET next_attempt_at=now()-interval '1 second'");
        assertEquals(1, store.pending("alice").size());
    }
    @Test void repeatedAcknowledgementPreservesTimestampWithoutExtraSendCount() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var identifier = store.pending("alice").getFirst().notificationId();
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(identifier, identifier), List.of(identifier, identifier)));
        var acknowledgedAt = jdbc.queryForObject("SELECT acknowledged_at FROM notification_deliveries", java.sql.Timestamp.class);
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(), List.of(identifier)));
        assertEquals(acknowledgedAt, jdbc.queryForObject("SELECT acknowledged_at FROM notification_deliveries", java.sql.Timestamp.class));
        assertEquals(1L, jdbc.queryForObject("SELECT sum(attempts) FROM notification_deliveries", Long.class));
        assertTrue(store.pending("alice").isEmpty());
    }
    @Test void customPlanIsScopedToDispatchTransaction() {
        var before = jdbc.queryForObject("SHOW plan_cache_mode", String.class);
        tx.executeWithoutResult(status -> {
            store.dispatchBatch("alice", List.of(), List.of());
            assertEquals(before, jdbc.queryForObject("SHOW plan_cache_mode", String.class));
            store.dispatchBatch("alice", List.of(UUID.randomUUID().toString()), List.of());
            assertEquals("force_custom_plan", jdbc.queryForObject("SHOW plan_cache_mode", String.class));
        });
        assertEquals(before, jdbc.queryForObject("SHOW plan_cache_mode", String.class));
    }
    @Test void configuredDispatchBatchBoundsQueryWithoutLosingRemainder() {
        var skus = java.util.stream.IntStream.range(0, 600).mapToObj(index -> "SKU-" + index).toList();
        subscribe("alice", skus.toArray(String[]::new));
        accept(Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now().plusSeconds(1), skus.stream().map(sku -> new SaleCompleted.Item(sku, 1)).toList())));
        var first = store.pending("alice");
        assertEquals(512, first.size());
        var identifiers = first.stream().map(Protocol.Delivery::notificationId).toList();
        var remaining = tx.execute(status -> store.dispatchBatch("alice", identifiers, identifiers));
        assertEquals(88, remaining.size());
        org.springframework.test.util.ReflectionTestUtils.setField(store, "dispatchBatchSize", 32);
        assertEquals(32, store.pending("alice").size());
        assertEquals(88L, jdbc.queryForObject("SELECT count(*) FROM notification_deliveries WHERE acknowledged_at IS NULL", Long.class));
    }
    @Test void failedDispatchCycleRollsBackSendAndAckTogether() {
        subscribe("alice", "A");
        subscribe("bob", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var aliceId = store.pending("alice").getFirst().notificationId();
        var bobId = store.pending("bob").getFirst().notificationId();
        assertThrows(IllegalArgumentException.class, () -> tx.execute(status ->
                store.dispatchBatch("alice", List.of(aliceId), List.of(aliceId, bobId))));
        assertEquals(0L, jdbc.queryForObject("SELECT sum(attempts) FROM notification_deliveries", Long.class));
        assertEquals(1, store.pending("alice").size());
        assertEquals(1, store.pending("bob").size());
    }
    @Test void nativeIdentifierUsesSixteenBytesWithoutChangingWireIdentity() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var identifier = store.pending("alice").getFirst().notificationId();
        tx.executeWithoutResult(status -> {
            assertTrue(store.dispatchBatch("alice", List.of(identifier), List.of()).isEmpty());
            assertEquals(16, jdbc.queryForObject("SELECT pg_column_size(notification_id) FROM notification_deliveries", Integer.class));
        });
        assertEquals(identifier, jdbc.queryForObject("SELECT notification_id FROM notification_deliveries", String.class));
        assertEquals(1L, store.metrics().get("send_attempts"));
        assertEquals(1L, store.metrics().get("pending"));
    }
    @Test void retryDeadlineFilteringDoesNotHideReadyRowsBeyondFirstBatch() {
        var skus = java.util.stream.IntStream.range(0, 600).mapToObj(index -> "SKU-" + index).toList();
        subscribe("alice", skus.toArray(String[]::new));
        accept(Json.write(new SaleCompleted(1, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                Instant.now().plusSeconds(1), skus.stream().map(sku -> new SaleCompleted.Item(sku, 1)).toList())));
        var blocked = store.pending("alice").stream().map(Protocol.Delivery::notificationId).toList();
        jdbc.update("UPDATE notification_deliveries SET next_attempt_at=now()+interval '1 hour' WHERE notification_id IN (SELECT jsonb_array_elements_text(?::jsonb)::uuid)", Json.write(blocked));
        for (int capacity : List.of(0, 16)) {
            store.configurePayloadCache(capacity);
            var ready = store.pending("alice");
            assertEquals(88, ready.size());
            assertTrue(ready.stream().noneMatch(delivery -> blocked.contains(delivery.notificationId())));
        }
        jdbc.update("UPDATE notification_deliveries SET next_attempt_at=now()-interval '1 second'");
        assertEquals(blocked, store.pending("alice").stream().map(Protocol.Delivery::notificationId).toList());
    }
    @Test void indexMigrationIsRepeatableAndPreservesRetryAndAckState() throws Exception {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        var identifier = store.pending("alice").getFirst().notificationId();
        tx.executeWithoutResult(status -> store.dispatchBatch("alice", List.of(identifier), List.of(identifier)));
        jdbc.execute("ALTER TABLE notification_deliveries ALTER COLUMN notification_id DROP DEFAULT; ALTER TABLE notification_deliveries ALTER COLUMN notification_id TYPE varchar(36) USING notification_id::text; ALTER TABLE notification_deliveries ALTER COLUMN notification_id SET DEFAULT notification_ordered_uuid()");
        var before = jdbc.queryForList("SELECT notification_id::text,acknowledged_at,next_attempt_at,attempts FROM notification_deliveries");
        for (int repeat = 0; repeat < 2; repeat++) jdbc.execute(Files.readString(Path.of("database/schema.sql")));
        assertEquals(before, jdbc.queryForList("SELECT notification_id::text,acknowledged_at,next_attempt_at,attempts FROM notification_deliveries"));
        assertEquals("uuid", jdbc.queryForObject("SELECT pg_typeof(notification_id)::text FROM notification_deliveries", String.class));
        String definition = jdbc.queryForObject("SELECT pg_get_indexdef('deliveries_dispatch'::regclass)", String.class);
        assertTrue(definition.contains("(subscriber_id, next_attempt_at, notification_id)"));
        assertTrue(store.pending("alice").isEmpty());
    }
    @Test void batchSendUpdatesRetryStateAndRetainsUnacknowledgedMessages() {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)), event("A", 2, Instant.now().plusSeconds(1)));
        var deliveries = store.pending("alice");
        store.sent(deliveries.stream().map(Protocol.Delivery::notificationId).toList());
        assertTrue(store.pending("alice").isEmpty());
        assertEquals(2L, jdbc.queryForObject("SELECT sum(attempts) FROM notification_deliveries", Long.class));
        jdbc.update("UPDATE notification_deliveries SET next_attempt_at=now()-interval '1 second'");
        assertEquals(2, store.pending("alice").size());
    }
    @Test void invalidLegacyIdentifierRollsBackTypeMigration() throws Exception {
        subscribe("alice", "A");
        accept(event("A", 1, Instant.now().plusSeconds(1)));
        jdbc.execute("ALTER TABLE notification_deliveries ALTER COLUMN notification_id DROP DEFAULT; ALTER TABLE notification_deliveries ALTER COLUMN notification_id TYPE varchar(36) USING notification_id::text; UPDATE notification_deliveries SET notification_id='invalid-legacy-identifier'");
        assertThrows(RuntimeException.class, () -> jdbc.execute(Files.readString(Path.of("database/schema.sql"))));
        assertEquals("character varying", jdbc.queryForObject("SELECT pg_typeof(notification_id)::text FROM notification_deliveries", String.class));
        assertEquals("invalid-legacy-identifier", jdbc.queryForObject("SELECT notification_id FROM notification_deliveries", String.class));
        assertEquals(1L, store.metrics().get("pending"));
    }
}
