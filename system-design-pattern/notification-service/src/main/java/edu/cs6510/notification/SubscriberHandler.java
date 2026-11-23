package edu.cs6510.notification;

import edu.cs6510.events.Json;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import java.util.Map;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// WebSocket 处理器，接收订阅者的 SUBSCRIBE、ACK、ACKS 消息，向订阅者发送 SALE_BATCH 消息
// 调用 NotificationStore.subscribe(...)，并将 WebSocket 连接关联到该订阅者
// 调用 DispatchDatabase.nextBatch(...)，获取待投递的通知，向订阅者发送 SALE_BATCH 消息
// 调用 DispatchDatabase.sent(...)，将已发送的通知标记为已投递
// 订阅者客户端：记录日志、发送 ACK
@Component
public class SubscriberHandler extends TextWebSocketHandler {
    private static final class RetryableChannelException extends IllegalArgumentException {
        final String code;
        RetryableChannelException(String code, String message) { super(message); this.code = code; }
    }
    private static final Logger log = LoggerFactory.getLogger(SubscriberHandler.class);
    // 服务端为每条连接保存独立状态，每条连接都有独立的：WebSocket session。订阅者 ID。ACK 队列。投递状态。
    //WebSocket 连接，保存订阅者 ID、是否批量投递、空闲轮询间隔、ACK 队列
    private static final class Connection {
        final WebSocketSession session;
        final long opened = System.nanoTime();
        volatile String subscriberId;
        boolean batchDeliveries;
        int idleDelayMillis = 200;
        final ArrayBlockingQueue<String> acknowledgements = new ArrayBlockingQueue<>(1024);
        Connection(WebSocketSession session) {
            this.session = new ConcurrentWebSocketSessionDecorator(session, 5000, 65536);
        }
    }
    private final NotificationStore store;
    private final DispatchDatabase dispatchDatabase;
    private final int maxIdlePollMillis;
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final Map<String, String> activeSubscribers = new ConcurrentHashMap<>();
    private final ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
    public SubscriberHandler(NotificationStore store, DispatchDatabase dispatchDatabase,
            @org.springframework.beans.factory.annotation.Value("${notification.max-idle-poll-ms:1000}") int maxIdlePollMillis) {
        if (maxIdlePollMillis < 200 || maxIdlePollMillis > 1000)
            throw new IllegalArgumentException("Maximum idle poll interval must be between 200 and 1000 ms");
        this.store = store;
        this.dispatchDatabase = dispatchDatabase;
        this.maxIdlePollMillis = maxIdlePollMillis;
    }
//新连接建立后，检查连接数是否超过 120，如果超过则关闭连接。
// 为每个session创建 Connection 对象，保存 WebSocket session，并提交 dispatch(...) 任务处理该连接
    @Override
    public synchronized void afterConnectionEstablished(WebSocketSession session) throws Exception {
        if (connections.size() >= 120) {
            session.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        session.setTextMessageSizeLimit(262144);
        var connection = new Connection(session);
        connections.put(session.getId(), connection);
        senders.submit(() -> dispatch(connection));
    }
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        var connection = connections.get(session.getId());
        if (connection == null) return;
        try {// 处理订阅者的 SUBSCRIBE、ACK、ACKS 消息 
            var command = Json.read(message.getPayload(), Protocol.Command.class);
            //把连接绑定到订阅者身份，校验并注册订阅，将 subscriberId 与 sessionId 关联，在当前服务实例中，拒绝同一订阅者同时建立第二条活动通道
            if ("SUBSCRIBE".equals(command.type()) && connection.subscriberId == null) {
                store.subscribe(command.subscriberId(), command.token(), command.skus());
                if (activeSubscribers.putIfAbsent(command.subscriberId(), session.getId()) != null)
                    throw new RetryableChannelException("SUBSCRIBER_BUSY", "Subscriber already has an active channel");
                connection.batchDeliveries = Boolean.TRUE.equals(command.batchDeliveries());
                connection.subscriberId = command.subscriberId();
                send(connection, Map.of("type", "SUBSCRIBED", "subscriberId", command.subscriberId()));
            } else if (("ACK".equals(command.type()) || "ACKS".equals(command.type())) && connection.subscriberId != null) {
                var identifiers = acknowledgementIds(command);
                if (connection.acknowledgements.remainingCapacity() < identifiers.size())
                    throw new RetryableChannelException("CHANNEL_BACKPRESSURE", "Channel acknowledgement buffer full");
                connection.acknowledgements.addAll(identifiers);
            } else {
                throw new IllegalArgumentException("SUBSCRIBE first, then ACK notifications");
            }
        } catch (Exception failure) {
            String code = failure instanceof RetryableChannelException retryable ? retryable.code
                    : failure instanceof IllegalArgumentException ? "INVALID_REQUEST" : "TEMPORARY_FAILURE";
            send(connection, Map.of("type", "ERROR", "code", code, "message",
                    failure instanceof IllegalArgumentException ? failure.getMessage() : "Notification operation failed"));
            session.close(CloseStatus.POLICY_VIOLATION);
        }
    }
    static java.util.List<String> acknowledgementIds(Protocol.Command command) {
        var identifiers = "ACK".equals(command.type())
                ? java.util.Collections.singletonList(command.notificationId()) : command.notificationIds();
        if (identifiers == null || identifiers.isEmpty() || identifiers.size() > 64
                || identifiers.stream().anyMatch(id -> id == null || id.length() != 36))
            throw new IllegalArgumentException("Invalid ACK batch");
        for (String identifier : identifiers) java.util.UUID.fromString(identifier);
        return java.util.List.copyOf(identifiers);
    }
    //只查询该订阅者的通知，dispatch(connection) 使用当前连接绑定的身份，然后按该身份取得通知，实际的数据库筛选位于NotificationStore244
    private void dispatch(Connection connection) {
        var sent = new ArrayList<String>();
        try {
            while (connection.session.isOpen() && !Thread.currentThread().isInterrupted()) {
                String subscriberId = connection.subscriberId;
                if (subscriberId == null) {
                    if (System.nanoTime() - connection.opened > 10_000_000_000L) {
                        connection.session.close(CloseStatus.POLICY_VIOLATION);
                        return;
                    }
                } else {
                    var acknowledgements = new ArrayList<String>();
                    var deliveries = dispatchDatabase.nextBatch(subscriberId, sent, () -> {
                        connection.acknowledgements.drainTo(acknowledgements);
                        return acknowledgements;
                    });
                    boolean hadWrites = !sent.isEmpty() || !acknowledgements.isEmpty();
                    connection.idleDelayMillis = nextIdleDelay(connection.idleDelayMillis,
                            hadWrites || !deliveries.isEmpty(), maxIdlePollMillis);
                    sent.clear();
                    if (connection.batchDeliveries) {
                        for (int offset = 0; offset < deliveries.size(); offset += 32) {
                            var batch = deliveries.subList(offset, Math.min(offset + 32, deliveries.size()));
                            send(connection, deliveryBatch(batch));//在对应连接上批量发送
                            for (var delivery : batch) sent.add(delivery.notificationId());
                        }
                    } else {
                        for (var delivery : deliveries) {
                            send(connection, delivery.forDispatch());//或者逐条发送
                            sent.add(delivery.notificationId());
                        }
                    }
                    if (!deliveries.isEmpty()) continue;
                }
                Thread.sleep(subscriberId == null ? 200 : connection.idleDelayMillis);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception failure) {
            log.warn("Subscriber channel failed; unacknowledged deliveries retained: {}", failure.toString());
        } finally {
            try {
                dispatchDatabase.sent(sent);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception failure) {
                log.warn("Send accounting failed; durable notifications remain retryable: {}", failure.toString());
            }
            try { connection.session.close(); } catch (Exception ignored) {}
            remove(connection.session.getId());
        }
    }
    static int nextIdleDelay(int previous, boolean active, int maximum) {
        return active ? 200 : Math.min(maximum, previous * 2);
    }
    static Map<String, Object> deliveryBatch(java.util.List<Protocol.Delivery> deliveries) {
        if (deliveries.isEmpty() || deliveries.size() > 32)
            throw new IllegalArgumentException("Invalid delivery batch size");
        return Map.of("type", "SALE_BATCH", "deliveries", deliveries.stream().map(Protocol.Delivery::forDispatch).toList());
    }
    private static void send(Connection connection, Object payload) throws Exception {
        connection.session.sendMessage(new TextMessage(Json.write(payload)));
    }
    private void remove(String sessionId) {
        var connection = connections.remove(sessionId);
        if (connection != null && connection.subscriberId != null)
            activeSubscribers.remove(connection.subscriberId, sessionId);
    }
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        remove(session.getId());
    }
    public int connectedUsers() { return activeSubscribers.size(); }
    @PreDestroy
    public void shutdown() {
        senders.shutdownNow();
        for (var connection : connections.values())
            try { connection.session.close(CloseStatus.GOING_AWAY); } catch (Exception ignored) {}
    }
}
