package edu.cs6510.subscriber;

import edu.cs6510.events.Json;
import java.io.BufferedWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
//客户端选择 SKU line 52、发送订阅请求 line120
public final class SubscriberMain {
    public record Options(URI url, int subscribers, int seconds, List<String> skus, String prefix, Path output) {}
    public static Options parse(String[] args) {
        var values = new HashMap<String, String>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 >= args.length || !Set.of("--url", "--subscribers", "--seconds", "--skus", "--prefix", "--output").contains(args[index]))
                throw new IllegalArgumentException("Expected --url/--subscribers/--seconds/--skus/--prefix/--output VALUE");
            values.put(args[index], args[index + 1]);
        }//create a configurable number of threads
        int count = Integer.parseInt(values.getOrDefault("--subscribers", "10"));//配置订阅者数量
        int seconds = Integer.parseInt(values.getOrDefault("--seconds", "60"));
        var skus = Arrays.stream(values.getOrDefault("--skus", "SKU-000001").split(",", -1)).map(String::trim).distinct().toList();
        String prefix = values.getOrDefault("--prefix", "demo");
        URI url = URI.create(values.getOrDefault("--url", "ws://127.0.0.1:8185/notifications"));
        if (count < 1 || count > 100 || seconds < 1 || !prefix.matches("[a-zA-Z0-9_-]{1,50}")
                || skus.isEmpty() || skus.size() > 5000 || skus.stream().anyMatch(sku -> sku.isBlank() || sku.length() > 32)
                || !Set.of("ws", "wss").contains(url.getScheme()))
            throw new IllegalArgumentException("Invalid options; subscribers must be 1..100");
        return new Options(url, count, seconds, skus, prefix, Path.of(values.getOrDefault("--output", "target/subscribers")));
    }
    public static void main(String[] args) throws Exception {
        Options options = parse(args);
        Files.createDirectories(options.output());
        Files.deleteIfExists(options.output().resolve("ready.json"));
        var stop = new AtomicBoolean();
        var ready = new CountDownLatch(options.subscribers());
        var failures = new ConcurrentLinkedQueue<Throwable>();
        List<Thread> threads = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.set(true);
            threads.forEach(Thread::interrupt);
        }));
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            for (int index = 0; index < options.subscribers(); index++) {
                String id = options.prefix() + "-" + index;
                //为每个测试订阅者选择 SKU；偶数用户订阅全部传入 SKU，奇数用户订阅其中一个
                //For testing, the subscriber application deliberately mixes subscription patterns:
                // Even-numbered subscribers subscribe to all supplied SKUs.
                // Odd-numbered subscribers subscribe to one supplied SKU.
                List<String> subscribed = index % 2 == 0 ? options.skus() : List.of(options.skus().get(index % options.skus().size()));
                //为每个订阅者创建线程，每个线程执行自己的 runSubscriber(...)，负责该订阅者的连接、订阅和接收流程
                Thread thread = Thread.ofPlatform().name(id).unstarted(() -> {
                    try { runSubscriber(http, options, id, subscribed, ready, stop); }
                    catch (Exception failure) { failures.add(failure); System.err.println(id + ": " + failure); }
                });
                threads.add(thread);
                thread.start();
            }
            try {
                long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (!ready.await(250, TimeUnit.MILLISECONDS)) {
                    checkFailures(failures);
                    if (System.nanoTime() >= readyDeadline) throw new IllegalStateException("Not all subscribers became ready");
                }
                checkFailures(failures);
                Files.writeString(options.output().resolve("ready.json"), Json.write(Map.of("subscribers", options.subscribers(), "at", Instant.now())));
                System.out.println("READY subscribers=" + options.subscribers() + " output=" + options.output());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(options.seconds());
                while (System.nanoTime() < deadline) {
                    checkFailures(failures);
                    Thread.sleep(250);
                }
            } finally {
                stop.set(true);
                threads.forEach(Thread::interrupt);
                for (Thread thread : threads) thread.join(5000);
            }
        }
        if (!failures.isEmpty()) throw new IllegalStateException("Subscriber failed", failures.peek());
    }
    static void checkFailures(Queue<Throwable> failures) {
        if (!failures.isEmpty()) throw new IllegalStateException("Subscriber failed", failures.peek());
    }
    static boolean retryableError(Object code) {
        return code instanceof String text && Set.of("SUBSCRIBER_BUSY", "CHANNEL_BACKPRESSURE", "TEMPORARY_FAILURE").contains(text);
    }
    private static void runSubscriber(HttpClient http, Options options, String id, List<String> skus,
            CountDownLatch ready, AtomicBoolean stop) throws Exception {
        Set<String> subscribedSkus = Set.copyOf(skus);
        Path tokenPath = options.output().resolve(id + ".token");
        String token;
        if (Files.exists(tokenPath)) token = Files.readString(tokenPath).trim();
        else {
            token = UUID.randomUUID().toString() + UUID.randomUUID();
            Files.writeString(tokenPath, token, StandardOpenOption.CREATE_NEW);
            try { Files.setPosixFilePermissions(tokenPath, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); }
            catch (UnsupportedOperationException ignored) {}
        }
        Files.writeString(options.output().resolve(id + ".subscription.json"), Json.write(Map.of("subscriberId", id, "skus", skus)));
        //为每个订阅者创建日志文件，记录接收到的通知 ID，避免重复处理。不同订阅者使用不同的日志文件。JSONL 表示每一行是一条 JSON 记录
        //实际写入逻辑在ReceiptBatch.java：31 record(...) 会检查 notification ID，避免重复记录，添加 receivedAt 接收时间，将通知写成一行 JSON
        Path logPath = options.output().resolve(id + ".jsonl");
        Set<UUID> seen = new HashSet<>();
        if (Files.exists(logPath)) {
            try (var lines = Files.lines(logPath)) {
                lines.filter(line -> !line.isBlank()).forEach(line -> {
                    var record = Json.read(line, Map.class);
                    seen.add(UUID.fromString((String) record.get("notificationId")));
                });
            }
        }
        boolean announced = false;
        try (BufferedWriter log = Files.newBufferedWriter(logPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            while (!stop.get()) {//每个订阅者创建自己的连接，服务端为每条连接保存独立状态SubscriberHandler.java:29
                var listener = new Listener();
                var receipts = new ReceiptBatch(log, seen);
                WebSocket socket = null;
                try {
                    socket = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
                    //订阅者通过 WebSocket 发送包含 subscriberId、`token`、`skus` 的 SUBSCRIBE 消息，buildAsync建立一个独立 WebSocket
                            .buildAsync(options.url(), listener).get(10, TimeUnit.SECONDS);
                    socket.sendText(Json.write(Map.of("type", "SUBSCRIBE", "subscriberId", id, "token", token, "skus", skus,
                            "batchDeliveries", true)), true).join();
                    while (!stop.get()) {
                        String body = listener.messages.poll(receipts.isEmpty() ? 1000 : 20, TimeUnit.MILLISECONDS);
                        if (body == null) {
                            receipts.flush(socket);
                            if (listener.closed) break;
                            continue;
                        }
                        var message = Json.read(body, Map.class);
                        String type = (String) message.get("type");
                        if ("ERROR".equals(type)) {
                            if (retryableError(message.get("code")))
                                throw new IllegalStateException(String.valueOf(message.get("message")));
                            throw new IllegalArgumentException(String.valueOf(message.get("message")));
                        }
                        if ("SUBSCRIBED".equals(type) && !announced) {
                            ready.countDown();
                            announced = true;
                        }
                        for (var sale : salesInMessage(message)) {
                            if (!id.equals(sale.get("subscriberId")) || !subscribedSkus.contains(sale.get("sku")))
                                throw new IllegalArgumentException("Notification routed to wrong subscriber");
                            receipts.record(sale);
                            if (receipts.due()) receipts.flush(socket);
                        }
                        socket.request(1);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (IllegalArgumentException fatal) {
                    throw fatal;
                } catch (java.io.IOException fatal) {
                    throw fatal;
                } catch (Exception transientFailure) {
                    if (!stop.get()) System.err.println(id + " reconnecting: " + transientFailure.getClass().getSimpleName());
                } finally {
                    try { receipts.persist(); }
                    finally { if (socket != null) socket.abort(); }
                }
                if (!stop.get()) {
                    try { Thread.sleep(1000); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
                }
            }
        }
    }
    static List<Map<?, ?>> salesInMessage(Map<?, ?> message) {
        if ("SALE".equals(message.get("type"))) return List.of(message);
        if (!"SALE_BATCH".equals(message.get("type"))) return List.of();
        if (!(message.get("deliveries") instanceof List<?> deliveries) || deliveries.isEmpty() || deliveries.size() > 32)
            throw new IllegalArgumentException("Invalid delivery batch");
        var result = new ArrayList<Map<?, ?>>();
        for (Object value : deliveries) {
            if (!(value instanceof Map<?, ?> sale) || !"SALE".equals(sale.get("type")))
                throw new IllegalArgumentException("Invalid sale in delivery batch");
            result.add(sale);
        }
        return result;
    }
    static final class Listener implements WebSocket.Listener {
        final BlockingQueue<String> messages = new ArrayBlockingQueue<>(8);
        private final StringBuilder fragments = new StringBuilder();
        volatile boolean closed;
        @Override public void onOpen(WebSocket socket) { socket.request(4); }
        @Override public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            fragments.append(data);
            if (fragments.length() > 65536) { closed = true; socket.abort(); return null; }
            if (last) {
                if (!messages.offer(fragments.toString())) { closed = true; socket.abort(); }
                fragments.setLength(0);
            } else {
                socket.request(1);
            }
            return null;
        }
        @Override public CompletionStage<?> onClose(WebSocket socket, int status, String reason) { closed = true; return null; }
        @Override public void onError(WebSocket socket, Throwable failure) { closed = true; }
    }
}
