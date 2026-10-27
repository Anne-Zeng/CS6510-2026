package edu.cs6510.events;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Kafka adapter for all inter-service traffic. Request/reply correlation is independent of
 * consumer threads, so Checkout can await Inventory without blocking its reply consumer.
 * Business handlers commit locally before replies/offsets. Crash redelivery is expected:
 * Checkout command_results, Inventory saga IDs and Analytics event IDs provide idempotency.
 */
public final class KafkaBus implements MessageBus {
    public record CorrelatedReply(String id, Reply reply) {}
    private final String bootstrap;
    private final String prefix;
    private final String service;
    private final String replies;
    private final long timeoutMs;
    private final int concurrency;
    private final KafkaProducer<String, String> producer;
    private final Map<String, CompletableFuture<Reply>> pending = new ConcurrentHashMap<>();
    private final List<KafkaConsumer<String, String>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<Thread> threads = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    public KafkaBus(String service) { this(service, System.getenv()); }

    public KafkaBus(String service, Map<String, String> environment) {
        if (!List.of("gateway", "catalog", "checkout", "inventory", "analytics").contains(service))
            throw new IllegalArgumentException("Unknown messaging service: " + service);
        this.service = service;
        bootstrap = environment.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        prefix = environment.getOrDefault("KAFKA_TOPIC_PREFIX", "selfcheckout");
        if (!prefix.matches("[a-zA-Z0-9_-]{1,80}")) throw new IllegalArgumentException("Invalid Kafka topic prefix");
        timeoutMs = number(environment, "KAFKA_REQUEST_TIMEOUT_MS", 8000, 100, 60000);
        int defaultWorkers = switch (service) { case "checkout" -> 24; case "inventory" -> 4; default -> 1; };
        int globalWorkers = number(environment, "KAFKA_CONSUMER_CONCURRENCY", defaultWorkers, 1, 32);
        concurrency = number(environment, "KAFKA_" + service.toUpperCase(java.util.Locale.ROOT) + "_CONCURRENCY", globalWorkers, 1, 32);
        int partitions = number(environment, "KAFKA_TOPIC_PARTITIONS", 24, 1, 64);
        replies = topic(service + ".replies");
        createTopics(partitions);
        var config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("client.id", prefix + "." + service + ".producer");
        config.put("acks", "all");
        config.put("enable.idempotence", "true");
        config.put("linger.ms", "0");
        config.put("buffer.memory", "8388608");
        config.put("delivery.timeout.ms", "15000");
        config.put("request.timeout.ms", "10000");
        config.put("max.block.ms", Long.toString(timeoutMs));
        producer = new KafkaProducer<>(config);
        var assigned = new CountDownLatch(1);
        try {
            // Each instance sees replies; only the instance owning the correlation ID completes it.
            startConsumer(replies, prefix + "." + service + ".replies." + UUID.randomUUID(), "latest", records -> {
                for (var record : records) {
                    var response = Json.read(record.value(), CorrelatedReply.class);
                    var future = pending.get(response.id());
                    if (future != null) future.complete(response.reply());
                }
            }, assigned);
            if (!assigned.await(45, TimeUnit.SECONDS)) throw new BusException("Kafka reply subscription did not become ready");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); close(); throw new BusException("Kafka startup interrupted", ex);
        } catch (RuntimeException ex) { close(); throw ex; }
    }

    private static int number(Map<String, String> env, String key, int fallback, int min, int max) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < min || value > max) throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
        return value;
    }

    private void createTopics(int partitions) {
        var topics = new ArrayList<NewTopic>();
        for (String name : List.of("gateway", "catalog", "checkout", "inventory", "analytics")) {
            topics.add(new NewTopic(topic(name + ".replies"), 1, (short) 1));
            if (!name.equals("gateway")) topics.add(new NewTopic(topic(name + ".requests"), partitions, (short) 1));
        }
        topics.add(new NewTopic(topic("scans"), 1, (short) 1));
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", "30000"))) {
            var created = admin.createTopics(topics);
            for (var future : created.values().values()) {
                try { future.get(35, TimeUnit.SECONDS); }
                catch (ExecutionException ex) { if (!(ex.getCause() instanceof TopicExistsException)) throw ex; }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new BusException("Kafka topic initialization failed; check KAFKA_BOOTSTRAP_SERVERS", ex);
        }
    }

    @Override public String topic(String suffix) { return prefix + "." + suffix; }

    @Override public Reply request(String target, String operation, String key, String body) {
        if (!running) throw new BusException("Kafka bus is closed");
        String id = UUID.randomUUID().toString();
        var future = new CompletableFuture<Reply>();
        pending.put(id, future);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            var request = new Request(id, operation, key, body, replies);
            producer.send(new ProducerRecord<>(topic(target + ".requests"), key, Json.write(request)))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Kafka business reply timed out");
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new BusException("Kafka request failed for " + target + "/" + operation, ex);
        } finally { pending.remove(id); }
    }

    @Override public void publish(String topic, String key, String body) {
        if (!running) throw new BusException("Kafka bus is closed");
        try { producer.send(new ProducerRecord<>(topic, key, body)).get(timeoutMs, TimeUnit.MILLISECONDS); }
        catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new BusException("Kafka publication not confirmed; retain outbox", ex);
        }
    }

    @Override public void serve(String name, Function<Request, Reply> handler) {
        if (!service.equals(name)) throw new IllegalArgumentException("A service can only consume its own commands");
        for (int worker = 0; worker < concurrency; worker++) {
            startConsumer(topic(name + ".requests"), prefix + "." + name + ".commands", "earliest", records -> {
                for (var record : records) {
                    var request = Json.read(record.value(), Request.class);
                    if (!request.replyTo().matches(java.util.regex.Pattern.quote(prefix) + "\\.(gateway|catalog|checkout|inventory|analytics)\\.replies"))
                        throw new IllegalArgumentException("Invalid reply topic");
                    Reply reply = handler.apply(request);
                    publish(request.replyTo(), request.id(), Json.write(new CorrelatedReply(request.id(), reply)));
                }
            }, null);
        }
    }

    @Override public void subscribe(String topic, String group, Consumer<List<String>> handler) {
        startConsumer(topic, prefix + "." + group, "earliest", records -> {
            var values = new ArrayList<String>();
            for (var record : records) values.add(record.value());
            handler.accept(values);
        }, null);
    }

    private void startConsumer(String topic, String group, String reset,
                               Consumer<ConsumerRecords<String, String>> callback, CountDownLatch assigned) {
        var config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("group.id", group);
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", reset);
        // A command may await another service. Bound each command poll to one such wait.
        config.put("max.poll.records", topic.endsWith(".requests") ? "1" : "32");
        config.put("fetch.max.wait.ms", "5");
        config.put("max.partition.fetch.bytes", "1048576");
        config.put("max.poll.interval.ms", "300000");
        var consumer = new KafkaConsumer<String, String>(config);
        consumers.add(consumer);
        Thread thread = Thread.ofPlatform().daemon(true).name(group + "-" + threads.size()).unstarted(() -> {
            var completed = new java.util.HashMap<TopicPartition, OffsetAndMetadata>();
            try {
                consumer.subscribe(List.of(topic));
                while (running) {
                    try {
                    var records = consumer.poll(Duration.ofMillis(100));
                    if (assigned != null && !consumer.assignment().isEmpty()) {
                        for (var partition : consumer.assignment()) consumer.position(partition);
                        assigned.countDown();
                    }
                    if (records.isEmpty()) continue;
                    try {
                        callback.accept(records);
                        // Replies belong to this process's pending futures and need no durable group offset.
                        // Other consumers commit only completed work. Async commits avoid serializing every
                        // business operation behind an extra coordinator round trip. A failed commit can
                        // cause safe redelivery; never retry an older offset over a newer one.
                        if (!topic.endsWith(".replies")) {
                            for (var partition : records.partitions()) {
                                var batch = records.records(partition);
                                completed.put(partition, new OffsetAndMetadata(batch.getLast().offset() + 1));
                            }
                            consumer.commitAsync((offsets, failure) -> {
                                if (failure != null && running)
                                    System.err.println("Kafka offset commit will be retried by subsequent progress: " + topic);
                            });
                        }
                    } catch (RuntimeException ex) {
                        if (!running) break;
                        System.err.println("Kafka processing retained for retry: " + topic + " / " + ex.getClass().getSimpleName());
                        var owned = consumer.assignment();
                        for (TopicPartition partition : records.partitions()) {
                            if (!owned.contains(partition)) continue;
                            var batch = records.records(partition);
                            consumer.seek(partition, batch.getFirst().offset());
                        }
                        try { Thread.sleep(500); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
                    }
                    } catch (WakeupException ex) {
                        if (!running) break;
                    } catch (org.apache.kafka.common.errors.RetriableException | org.apache.kafka.clients.consumer.CommitFailedException ex) {
                        if (!running) break;
                        System.err.println("Kafka consumer reconnecting: " + topic + " / " + ex.getClass().getSimpleName());
                    }
                }
            } catch (WakeupException ex) {
                if (running) System.err.println("Unexpected Kafka consumer wakeup: " + topic);
            } finally {
                // Use explicit completed offsets: a wakeup may have interrupted a polled but unfinished record.
                if (!completed.isEmpty()) {
                    try { consumer.commitSync(completed, Duration.ofSeconds(2)); }
                    catch (RuntimeException ignored) { /* Uncommitted work is replayed with stable IDs. */ }
                }
                consumer.close(Duration.ofSeconds(3));
            }
        });
        threads.add(thread); thread.start();
    }

    @Override public void close() {
        if (!running) return;
        running = false;
        pending.values().forEach(future -> future.completeExceptionally(new BusException("Kafka bus closed")));
        consumers.forEach(KafkaConsumer::wakeup);
        for (Thread thread : threads) {
            try { thread.join(5000); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); break; }
        }
        producer.close(Duration.ofSeconds(3));
    }
}
