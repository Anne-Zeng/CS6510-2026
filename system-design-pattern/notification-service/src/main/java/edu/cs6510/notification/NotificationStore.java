package edu.cs6510.notification;

import edu.cs6510.events.Json;
import edu.cs6510.events.SaleCompleted;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.ArrayList;
// NotificationStore 是 Notification Service 的核心组件，负责接收销售事件，按 SKU 匹配订阅者，生成待投递通知，并提供投递和确认接口
@Repository
public class NotificationStore {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NotificationStore.class);
    @Value("${notification.stage-timing:false}")
    private boolean stageTiming;
    @Value("${notification.dispatch-batch-size:512}")
    private int dispatchBatchSize = 512;
    @Value("${notification.dispatch-custom-plan:true}")
    private boolean dispatchCustomPlan;
    @Value("${notification.coalesce-dispatch-writes:true}")
    private boolean coalesceDispatchWrites = true;
    private final java.util.concurrent.atomic.AtomicBoolean slowPlanCaptured = new java.util.concurrent.atomic.AtomicBoolean();
    private final JdbcTemplate jdbc;
    private final int maxUsers;
    private final long retryMillis;
    private SaleItemCache payloadCache = new SaleItemCache(0);
    @Value("${notification.payload-cache-size:0}")
    public void configurePayloadCache(int capacity) {
        payloadCache = new SaleItemCache(capacity);
    }
    public Map<String, Long> payloadCacheMetrics() { return payloadCache.metrics(); }
    public NotificationStore(JdbcTemplate jdbc,
            @Value("${notification.max-users:100}") int maxUsers,//The Notification Service defaults to: 100 subscribers, each of which can subscribe to up to 5,000 SKUs.
            @Value("${notification.retry-ms:2000}") long retryMillis) {
        if (maxUsers < 1 || maxUsers > 100 || retryMillis < 100)
            throw new IllegalArgumentException("Invalid notification configuration");
        this.jdbc = jdbc;
        this.maxUsers = maxUsers;
        this.retryMillis = retryMillis;
    }
    @jakarta.annotation.PostConstruct
    public void validateDispatchBatchSize() {
        if (dispatchBatchSize < 1 || dispatchBatchSize > 512)
            throw new IllegalArgumentException("Dispatch batch size must be between 1 and 512");
    }
    // 核心：校验并保存 SKU 订阅, 拒绝 null 或空 SKU 列表，保证至少一个 SKU; 使用 TreeSet 去除重复 SKU; 
    // 保存订阅者到 subscribers 表，将每个订阅 SKU 保存到 subscriptions 表；
    // 重连时核对凭证及原始 SKU 列表: 如果订阅者已存在，检查 token 是否匹配，SKU 列表是否相同
    // Input comes from A user registered interest: Records who wants to hear about which products, Main tables written	subscribers, subscriptions
    // l 59 register interest, Triggered by: a subscriber sending a WebSocket SUBSCRIBE command
    @Transactional
    public void subscribe(String subscriberId, String token, List<String> skus) {
        if (subscriberId == null || !subscriberId.matches("[a-zA-Z0-9_-]{1,64}")
                || token == null || token.length() < 32 || token.length() > 128
                || skus == null || skus.isEmpty() || skus.size() > 5000
                || skus.stream().anyMatch(sku -> sku == null || sku.isBlank() || sku.length() > 32))
            throw new IllegalArgumentException("Invalid subscriber, token or SKU list");
        jdbc.execute("SELECT pg_advisory_xact_lock(6510, 20)");
        String hashed = hash(token);
        var existing = jdbc.queryForList("SELECT token_hash FROM subscribers WHERE subscriber_id=?", String.class, subscriberId);
        if (!existing.isEmpty()) {
            if (!existing.getFirst().equals(hashed))
                throw new IllegalArgumentException("Subscriber credentials rejected");
            var stored = jdbc.queryForList("SELECT sku FROM subscriptions WHERE subscriber_id=?", String.class, subscriberId);
            if (!new TreeSet<>(stored).equals(new TreeSet<>(skus)))
                throw new IllegalArgumentException("Subscription is immutable; reconnect with the original SKU list");
            return;
        }
        if (jdbc.queryForObject("SELECT count(*) FROM subscribers", Long.class) >= maxUsers)
            throw new IllegalArgumentException("Subscriber capacity reached");
        jdbc.update("INSERT INTO subscribers(subscriber_id,token_hash) VALUES (?,?)", subscriberId, hashed);
        jdbc.batchUpdate("INSERT INTO subscriptions(subscriber_id,sku) VALUES (?,?)",
                new TreeSet<>(skus).stream().map(sku -> new Object[]{subscriberId, sku}).toList());
    }
    //校验 SaleCompleted 格式>写入 sale_inbox，识别重复或冲突事件>line 121 只为新事件保存 notification_sale_items>按 SKU 关联 subscriptions>生成 notification_deliveries
    //Outbox 提供的是可重试发布，不能保证消息只发送一次。Notification 的 inbox 去重保证重复销售事件不会再次生成一批通知
    // 1. 销售事件的唯一标识是 eventId，重复的 eventId 会被忽略
    // 2. 如果同一个 eventId 的 transactionId 或 payload 不同，则认为是冲突事件，抛出异常 line 105
    // 3. 对于新事件，将每个 item 的 SKU 与 subscriptions 表匹配，生成 notification_sale_items 和 notification_deliveries, Creates pending delivery records for eligible subscribers
    // 4. 只有订阅了该 SKU 的用户才会收到通知  销售发生后如何匹配？ line 127
    //Input comes from 	Checkout through Kafka: Processes sales and determines who should receive notifications, create deliveries(sale_inbox, sale_items, deliveries
    //Records the event in a durable inbox，Stores the sale’s SKU quantities， Matches each SKU against subscriptions，Creates delivery records for matching subscribers 
    // l 94 accept sales, Triggered by SalesEvents from Kafka: the Outbox Service sending a batch of sale events to the Notification Service
    //records contains serialized sales-event JSON strings, not subscription requests
    //This method creates durable pending notifications; it does not itself send WebSocket messages. The dispatch code sends them afterward.
    @Transactional
    public void accept(List<String> records) {
        if (records.isEmpty()) return;
        for (String body : records) Json.read(body, SaleCompleted.class);
        String batch = "[" + String.join(",", records) + "]";
        long lockStarted = System.nanoTime();
        jdbc.execute("SELECT pg_advisory_xact_lock(6510, 20)");
        long lockNanos = System.nanoTime() - lockStarted;
        long inboxStarted = System.nanoTime();
        var inserted = jdbc.queryForList("""
                INSERT INTO sale_inbox(event_id,transaction_id,payload)
                SELECT event->>'eventId',event->>'transactionId',event
                FROM jsonb_array_elements(?::jsonb) AS event
                ON CONFLICT DO NOTHING RETURNING event_id
                """, String.class, batch);
        if (inserted.size() != records.size()) {
            Boolean same = jdbc.queryForObject("""
                    SELECT NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements(?::jsonb) AS event
                        LEFT JOIN sale_inbox inbox ON inbox.event_id=event->>'eventId'
                        WHERE inbox.event_id IS NULL OR inbox.transaction_id<>event->>'transactionId'
                           OR inbox.payload<>event
                    )
                    """, Boolean.class, batch);
            if (!Boolean.TRUE.equals(same)) throw new IllegalArgumentException("Conflicting sale event");
        }
        long inboxNanos = System.nanoTime() - inboxStarted;
        long fanoutStarted = System.nanoTime();
        int insertedDeliveries = 0;
        if (!inserted.isEmpty()) {
            // line 124 s.sku = item.sku 就是“只通知订阅了该商品的用户
            insertedDeliveries = jdbc.update("""
                    WITH items AS (
                        INSERT INTO notification_sale_items(event_id,transaction_id,sku,quantity,completed_at)
                        SELECT inbox.event_id,inbox.transaction_id,item.sku,item.quantity,
                               (inbox.payload->>'completedAt')::timestamptz
                        FROM sale_inbox inbox
                        CROSS JOIN LATERAL jsonb_to_recordset(inbox.payload->'items') AS item(sku text,quantity integer)
                        WHERE inbox.event_id IN (SELECT jsonb_array_elements_text(?::jsonb))
                        RETURNING item_id,event_id,sku,completed_at
                    )
                    INSERT INTO notification_deliveries(item_id,subscriber_id)
                    SELECT item.item_id,s.subscriber_id FROM items item
                    JOIN subscriptions s ON s.sku=item.sku JOIN subscribers u USING(subscriber_id)
                    WHERE u.subscribed_at<=item.completed_at
                    ORDER BY s.subscriber_id,item.event_id,item.sku
                    """, Json.write(inserted));
        }
        long fanoutNanos = System.nanoTime() - fanoutStarted;
        if (stageTiming)
            log.info("NOTIFICATION_DB_TIMING {}", Json.write(Map.of(
                    "records", records.size(), "insertedSales", inserted.size(),
                    "insertedDeliveries", insertedDeliveries,
                    "lockMs", lockNanos / 1_000_000.0,
                    "inboxSqlMs", inboxNanos / 1_000_000.0,
                    "fanoutSqlMs", fanoutNanos / 1_000_000.0)));
    }
    @Transactional
    public List<Protocol.Delivery> dispatchBatch(String subscriberId, List<String> sentIds, List<String> acknowledgedIds) {
        if (dispatchCustomPlan && (!sentIds.isEmpty() || !acknowledgedIds.isEmpty()))
            jdbc.execute("SET LOCAL plan_cache_mode=force_custom_plan");
        long started = System.nanoTime();
        long sentAt = started;
        if (coalesceDispatchWrites) {
            updateDispatchState(subscriberId, sentIds, acknowledgedIds);
        } else {
            sent(sentIds);
            sentAt = System.nanoTime();
            acknowledgeBatch(subscriberId, acknowledgedIds);
        }
        long acknowledgedAt = System.nanoTime();
        if (stageTiming && !coalesceDispatchWrites && acknowledgedAt-sentAt > 100_000_000L && slowPlanCaptured.compareAndSet(false, true))
            captureSlowAckPlan(subscriberId, acknowledgedIds);
        var deliveries = pending(subscriberId);
        if (stageTiming) {
            var timing = new java.util.LinkedHashMap<String, Object>();
            timing.put("stateWriteMs", (acknowledgedAt-started)/1_000_000.0);
            timing.put("pendingMs", (System.nanoTime()-acknowledgedAt)/1_000_000.0);
            timing.put("sentCount", sentIds.size());
            timing.put("ackCount", acknowledgedIds.size());
            timing.put("fetched", deliveries.size());
            timing.put("coalesced", coalesceDispatchWrites);
            if (!coalesceDispatchWrites) {
                timing.put("sentMs", (sentAt-started)/1_000_000.0);
                timing.put("ackMs", (acknowledgedAt-sentAt)/1_000_000.0);
            }
            log.info("NOTIFICATION_DISPATCH_SQL {}", Json.write(timing));
        }
        return deliveries;
    }
    private record DispatchState(String id, boolean sent, boolean ack) {}

    private void updateDispatchState(String subscriberId, List<String> sentIds, List<String> acknowledgedIds) {
        var sent = new TreeSet<>(sentIds);
        var acknowledged = new TreeSet<>(acknowledgedIds);
        if (sent.size() > 512 || acknowledged.size() > 1024)
            throw new IllegalArgumentException("Dispatch state batch too large");
        var identifiers = new TreeSet<>(sent);
        identifiers.addAll(acknowledged);
        if (identifiers.isEmpty()) return;
        var state = identifiers.stream().map(id -> new DispatchState(id, sent.contains(id), acknowledged.contains(id))).toList();
        var updated = jdbc.queryForList("""
            UPDATE notification_deliveries AS delivery
            SET attempts=delivery.attempts+CASE WHEN state.sent THEN 1 ELSE 0 END,
                next_attempt_at=CASE WHEN state.sent AND NOT state.ack AND delivery.acknowledged_at IS NULL
                    THEN clock_timestamp()+(? * interval '1 millisecond') ELSE delivery.next_attempt_at END,
                acknowledged_at=CASE WHEN state.ack THEN COALESCE(delivery.acknowledged_at,clock_timestamp())
                    ELSE delivery.acknowledged_at END
            FROM jsonb_to_recordset(?::jsonb) AS state(id uuid,sent boolean,ack boolean)
            WHERE delivery.notification_id=state.id AND delivery.subscriber_id=?
            RETURNING delivery.notification_id
            """, String.class, retryMillis, Json.write(state), subscriberId);
        if (!new java.util.HashSet<>(updated).containsAll(acknowledged))
            throw new IllegalArgumentException("Unknown notification for subscriber");
    }
    private void captureSlowAckPlan(String subscriberId, List<String> acknowledgedIds) {
        jdbc.execute((org.springframework.jdbc.core.ConnectionCallback<Void>) connection -> {
            var savepoint = connection.setSavepoint();
            try (var statement = connection.createStatement()) {
                String name = null;
                long genericPlans = 0;
                long customPlans = 0;
                try (var prepared = statement.executeQuery("SELECT name,generic_plans,custom_plans FROM pg_prepared_statements WHERE statement LIKE 'UPDATE notification_deliveries SET acknowledged_at=%'")) {
                    if (prepared.next()) {
                        name = prepared.getString(1);
                        genericPlans = prepared.getLong(2);
                        customPlans = prepared.getLong(3);
                    }
                }
                if (name != null && name.matches("[a-zA-Z0-9_]+")) {
                    String query = "EXPLAIN (FORMAT JSON) EXECUTE \"" + name + "\" ('"
                            + subscriberId.replace("'", "''") + "','" + Json.write(acknowledgedIds).replace("'", "''") + "')";
                    try (var plan = statement.executeQuery(query)) {
                        if (plan.next()) log.info("NOTIFICATION_SLOW_ACK_PLAN {}", Json.write(Map.of(
                                "genericPlans", genericPlans, "customPlans", customPlans, "plan", plan.getString(1))));
                    }
                }
            } catch (java.sql.SQLException failure) {
                connection.rollback(savepoint);
                log.warn("Unable to capture diagnostic ACK plan: {}", failure.getMessage());
            } finally {
                connection.releaseSavepoint(savepoint);
            }
            return null;
        });
    }
    public List<Protocol.Delivery> pending(String subscriberId) {
        if (payloadCache.enabled()) return cachedPending(subscriberId);
        return jdbc.query("""
            WITH pending AS MATERIALIZED (
                SELECT notification_id,item_id,subscriber_id,next_attempt_at FROM notification_deliveries
                WHERE subscriber_id=? AND acknowledged_at IS NULL AND next_attempt_at<=now()
                ORDER BY next_attempt_at,notification_id LIMIT ?
            )
            SELECT pending.notification_id,pending.subscriber_id,item.event_id,item.transaction_id,
                   item.sku,item.quantity,item.completed_at
            FROM pending JOIN notification_sale_items item USING(item_id)
            ORDER BY pending.next_attempt_at,pending.notification_id
            """, (row, index) -> new Protocol.Delivery("SALE", row.getString("notification_id"),
                row.getString("event_id"), row.getString("subscriber_id"), row.getString("transaction_id"),
                row.getString("sku"), row.getInt("quantity"), row.getTimestamp("completed_at").toInstant(), null), subscriberId, dispatchBatchSize);
    }
    private record PendingItem(String notificationId, long itemId) {}
    private List<Protocol.Delivery> cachedPending(String subscriberId) {
        var pending = jdbc.query("""
                SELECT notification_id,item_id FROM notification_deliveries
                WHERE subscriber_id=? AND acknowledged_at IS NULL AND next_attempt_at<=now()
                ORDER BY next_attempt_at,notification_id LIMIT ?
                """, (row, index) -> new PendingItem(row.getString("notification_id"), row.getLong("item_id")),
                subscriberId, dispatchBatchSize);
        if (pending.isEmpty()) return List.of();
        var items = new java.util.HashMap<Long, SaleItemCache.Item>();
        var missing = new java.util.LinkedHashSet<Long>();
        for (var delivery : pending) {
            var item = payloadCache.get(delivery.itemId());
            if (item == null) missing.add(delivery.itemId()); else items.put(item.itemId(), item);
        }
        if (!missing.isEmpty()) {
            var loaded = jdbc.query("""
                    SELECT item_id,event_id,transaction_id,sku,quantity,completed_at FROM notification_sale_items
                    WHERE item_id IN (SELECT value::bigint FROM jsonb_array_elements_text(?::jsonb))
                    """, (row, index) -> new SaleItemCache.Item(row.getLong("item_id"), row.getString("event_id"),
                    row.getString("transaction_id"), row.getString("sku"), row.getInt("quantity"),
                    row.getTimestamp("completed_at").toInstant()), Json.write(missing));
            for (var item : loaded) {
                items.put(item.itemId(), item);
                payloadCache.put(item);
            }
        }
        return pending.stream().map(delivery -> {
            var item = items.get(delivery.itemId());
            if (item == null) throw new IllegalStateException("Missing durable notification payload");
            return new Protocol.Delivery("SALE", delivery.notificationId(), item.eventId(), subscriberId,
                    item.transactionId(), item.sku(), item.quantity(), item.completedAt(), null);
        }).toList();
    }
    public void sent(List<String> notificationIds) {
        if (notificationIds.isEmpty()) return;
        jdbc.update("""
            UPDATE notification_deliveries SET attempts=attempts+1,
            next_attempt_at=clock_timestamp()+(? * interval '1 millisecond')
            WHERE notification_id IN (SELECT jsonb_array_elements_text(?::jsonb)::uuid)
            """, retryMillis, Json.write(notificationIds));
    }
    @Transactional
    public void acknowledge(String subscriberId, String notificationId) {
        acknowledgeBatch(subscriberId, List.of(notificationId));
    }
    @Transactional
    public void acknowledgeBatch(String subscriberId, List<String> notificationIds) {
        if (notificationIds.isEmpty()) return;
        var uniqueIds = new ArrayList<>(new TreeSet<>(notificationIds));
        if (uniqueIds.size() > 1024) throw new IllegalArgumentException("ACK batch too large");
        int count = jdbc.update("""
            UPDATE notification_deliveries SET acknowledged_at=COALESCE(acknowledged_at,clock_timestamp())
            WHERE subscriber_id=? AND notification_id IN (SELECT jsonb_array_elements_text(?::jsonb)::uuid)
            """, subscriberId, Json.write(uniqueIds));
        if (count != uniqueIds.size()) throw new IllegalArgumentException("Unknown notification for subscriber");
    }
    public Map<String, Object> metrics() {
        return jdbc.queryForMap("""
            SELECT MAX(value) FILTER(WHERE name='subscribers') AS subscribers,
                   MAX(value) FILTER(WHERE name='sales') AS sales,
                   MAX(value) FILTER(WHERE name='notifications') AS notifications,
                   MAX(value) FILTER(WHERE name='notifications')-MAX(value) FILTER(WHERE name='acknowledged') AS pending,
                   MAX(value) FILTER(WHERE name='send_attempts') AS send_attempts
            FROM notification_metric_totals
            """);
    }
    private static String hash(String token) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
