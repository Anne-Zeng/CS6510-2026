package edu.cs6510.events;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
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
    private final Set<String> allowedReplyTopics;
    private final long timeoutMs;
    private final int concurrency;
    private final int fetchWaitMs;
    private final int eventFetchWaitMs;
    private final int commandPollMs;
    private final boolean headerEnvelopes;
    private final PerformanceLog performance;
    private final KafkaProducer<String, String> producer;
    private final ExecutorService commandExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final KeyedDispatcher commands = new KeyedDispatcher(commandExecutor);
    private final Map<String, CompletableFuture<Reply>> pending = new ConcurrentHashMap<>();
    private final List<KafkaConsumer<String, String>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<Thread> threads = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    public KafkaBus(String service) { this(service, System.getenv()); }

    public KafkaBus(String service, Map<String, String> environment) {
        if (!List.of("gateway", "catalog", "checkout", "inventory", "analytics").contains(service))
            throw new IllegalArgumentException("Unknown messaging service: " + service);
        this.service = service;
        // Optional stage timings help distinguish business/database work from broker/fetch waits.
        // Leave disabled for normal runs: the disabled path does not read the clock or allocate samples.
        performance = new PerformanceLog(service, Boolean.parseBoolean(environment.getOrDefault("KAFKA_PERFORMANCE_LOG", "false")));
        bootstrap = environment.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        prefix = environment.getOrDefault("KAFKA_TOPIC_PREFIX", "selfcheckout");
        if (!prefix.matches("[a-zA-Z0-9_-]{1,80}")) throw new IllegalArgumentException("Invalid Kafka topic prefix");
        timeoutMs = number(environment, "KAFKA_REQUEST_TIMEOUT_MS", 8000, 100, 60000);
        // With fetch.min.bytes=1 the broker returns as soon as a record arrives. The normal
        // 500ms empty-fetch wait avoids allocating/networking continuously on idle topics.
        fetchWaitMs = number(environment, "KAFKA_FETCH_MAX_WAIT_MS", 500, 0, 1000);
        eventFetchWaitMs = number(environment, "KAFKA_EVENT_FETCH_MAX_WAIT_MS", 500, 0, 1000);
        commandPollMs = number(environment, "KAFKA_COMMAND_POLL_MS", 100, 10, 1000);
        headerEnvelopes = Boolean.parseBoolean(environment.getOrDefault("KAFKA_HEADER_ENVELOPES", "true"));
        // Pollers own Kafka network connections. Business work runs separately on inexpensive virtual
        // threads, so adding stations does not require adding dozens of KafkaConsumer instances.
        int globalWorkers = number(environment, "KAFKA_CONSUMER_CONCURRENCY", 1, 1, 32);
        concurrency = number(environment, "KAFKA_" + service.toUpperCase(java.util.Locale.ROOT) + "_CONCURRENCY", globalWorkers, 1, 32);
        int partitions = number(environment, "KAFKA_TOPIC_PARTITIONS", 4, 1, 64);
        replies = topic(service + ".replies");
        allowedReplyTopics = Set.of(topic("gateway.replies"), topic("catalog.replies"), topic("checkout.replies"),
                topic("inventory.replies"), topic("analytics.replies"));
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
                    var response = KafkaEnvelope.reply(record);
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
        long started = performance.start();
        boolean successful = false;
        String id = UUID.randomUUID().toString();
        var future = new CompletableFuture<Reply>();
        pending.put(id, future);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            var request = new Request(id, operation, key, body, replies);
            // A business reply proves that the request reached its consumer. Waiting for the send
            // acknowledgment first only serializes the caller behind a redundant network wait.
            long sent = performance.start();
            producer.send(KafkaEnvelope.request(topic(target + ".requests"), request, headerEnvelopes), (metadata, failure) -> {
                performance.elapsed(operation, Stage.REQUEST_ACK, sent, failure != null);
                if (failure != null) future.completeExceptionally(failure);
            });
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Kafka business reply timed out");
            Reply reply = future.get(remaining, TimeUnit.NANOSECONDS);
            successful = true;
            return reply;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new BusException("Kafka request failed for " + target + "/" + operation, ex);
        } finally {
            pending.remove(id);
            performance.elapsed(operation, Stage.REQUEST_TOTAL, started, !successful);
            performance.completed();
        }
    }

    @Override public void publish(String topic, String key, String body) {
        if (!running) throw new BusException("Kafka bus is closed");
        try { publishAsync(topic, key, body).get(timeoutMs, TimeUnit.MILLISECONDS); }
        catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new BusException("Kafka publication not confirmed; retain outbox", ex);
        }
    }

    private CompletableFuture<Void> publishAsync(String topic, String key, String body) {
        return publishAsync(new ProducerRecord<>(topic, key, body));
    }

    private CompletableFuture<Void> publishAsync(ProducerRecord<String,String> record) {
        var confirmed = new CompletableFuture<Void>();
        try {
            producer.send(record, (metadata, failure) -> {
                if (failure == null) confirmed.complete(null);
                else confirmed.completeExceptionally(failure);
            });
        } catch (RuntimeException failure) { confirmed.completeExceptionally(failure); }
        return confirmed;
    }

    @Override public void serve(String name, Function<Request, Reply> handler) {
        if (!service.equals(name)) throw new IllegalArgumentException("A service can only consume its own commands");
        for (int worker = 0; worker < concurrency; worker++) {
            startCommandConsumer(topic(name + ".requests"), prefix + "." + name + ".commands", handler);
        }
    }

    private record Completed(TopicPartition partition, PartitionProgress generation, long offset, Throwable failure) {}

    /** Keep polling while commands wait for a database or another service. All KafkaConsumer calls
     * stay on this one thread; workers only enqueue outcomes after the reply's broker acknowledgment. */
    private void startCommandConsumer(String topic, String group, Function<Request, Reply> handler) {
        var consumer = createConsumer(topic, group, "earliest", 64);
        consumers.add(consumer);
        Thread thread = Thread.ofPlatform().daemon(true).name(group + "-" + threads.size()).unstarted(() -> {
            var progress = new HashMap<TopicPartition, PartitionProgress>();
            var finished = new ConcurrentLinkedQueue<Completed>();
            var paused = new HashSet<TopicPartition>();
            long lastCommit = System.nanoTime();
            try {
                consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        var offsets = completedOffsets(progress);
                        offsets.keySet().retainAll(partitions);
                        if (!offsets.isEmpty()) {
                            try { consumer.commitSync(offsets, Duration.ofSeconds(2)); }
                            catch (RuntimeException ignored) { /* Business IDs make replay safe. */ }
                        }
                        partitions.forEach(progress::remove);
                        paused.removeAll(partitions);
                    }
                    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        partitions.forEach(partition -> progress.computeIfAbsent(partition, ignored -> new PartitionProgress()));
                    }
                });
                while (running) {
                    try {
                        Completed done;
                        while ((done = finished.poll()) != null) {
                            // A late completion from an old assignment must not commit a new owner's work.
                            if (progress.get(done.partition()) != done.generation()) continue;
                            done.generation().finished(done.offset(), done.failure() == null, System.nanoTime());
                            if (done.failure() != null)
                                System.err.println("Kafka command retained for replay: " + topic + " / " + done.failure().getClass().getSimpleName());
                        }
                        long now = System.nanoTime();
                        for (var entry : progress.entrySet()) {
                            var partition = entry.getKey();
                            var state = entry.getValue();
                            if (state.retryReady(now)) consumer.seek(partition, state.restart());
                            boolean shouldPause = state.failed() || state.inFlight() >= 256;
                            if (shouldPause && paused.add(partition)) consumer.pause(List.of(partition));
                            else if (!shouldPause && state.inFlight() < 128 && paused.remove(partition))
                                consumer.resume(List.of(partition));
                        }
                        if (now - lastCommit >= TimeUnit.MILLISECONDS.toNanos(100)) {
                            var offsets = completedOffsets(progress);
                            if (!offsets.isEmpty()) consumer.commitAsync(offsets, (committed, failure) -> {
                                if (failure != null && running)
                                    System.err.println("Kafka command offsets will be retried with subsequent progress: " + topic);
                            });
                            lastCommit = now;
                        }
                        var records = consumer.poll(Duration.ofMillis(commandPollMs));
                        for (var record : records) {
                            var partition = new TopicPartition(record.topic(), record.partition());
                            var state = progress.computeIfAbsent(partition, ignored -> new PartitionProgress());
                            state.dispatched(record.offset());
                            String key = record.key() == null ? partition + ":" + record.offset() : record.key();
                            long queuedAt = performance.start();
                            // Producer timestamps use wall time; this is approximate cross-host clock age.
                            // Locally it reveals time before dispatch: producer, broker and consumer fetch.
                            long recordAge = performance.enabled && record.timestamp() >= 0
                                    ? TimeUnit.MILLISECONDS.toNanos(Math.max(0, System.currentTimeMillis() - record.timestamp())) : 0;
                            commands.submit(key, () -> {
                                long dispatchedAt = performance.start();
                                var request = KafkaEnvelope.request(record);
                                performance.observe(request.operation(), Stage.DISPATCH_QUEUE, dispatchedAt - queuedAt, false);
                                performance.observe(request.operation(), Stage.RECORD_AGE, recordAge, false);
                                if (!allowedReplyTopics.contains(request.replyTo()))
                                    throw new IllegalArgumentException("Invalid reply topic");
                                long handlerStarted = performance.start();
                                Reply reply;
                                boolean handled = false;
                                try {
                                    reply = handler.apply(request);
                                    handled = true;
                                } finally {
                                    performance.elapsed(request.operation(), Stage.HANDLER, handlerStarted, !handled);
                                }
                                long replied = performance.start();
                                // Match the caller's encoding, including retained legacy commands.
                                var publication = publishAsync(KafkaEnvelope.reply(request.replyTo(),
                                        new CorrelatedReply(request.id(), reply), KafkaEnvelope.hasHeaders(record)));
                                return performance.enabled ? publication.whenComplete((ignored, failure) ->
                                        performance.elapsed(request.operation(), Stage.REPLY_ACK, replied, failure != null)) : publication;
                            }).whenComplete((ignored, failure) -> {
                                finished.add(new Completed(partition, state, record.offset(), failure));
                                performance.completed();
                            });
                        }
                    } catch (WakeupException ex) {
                        if (!running) break;
                    } catch (org.apache.kafka.common.errors.RetriableException | org.apache.kafka.clients.consumer.CommitFailedException ex) {
                        if (!running) break;
                        System.err.println("Kafka command consumer reconnecting: " + topic + " / " + ex.getClass().getSimpleName());
                    }
                }
            } finally {
                var offsets = completedOffsets(progress);
                if (!offsets.isEmpty()) {
                    try { consumer.commitSync(offsets, Duration.ofSeconds(2)); }
                    catch (RuntimeException ignored) { /* Unfinished records remain replayable. */ }
                }
                consumer.close(Duration.ofSeconds(3));
            }
        });
        threads.add(thread); thread.start();
    }

    private static Map<TopicPartition, OffsetAndMetadata> completedOffsets(Map<TopicPartition, PartitionProgress> progress) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        progress.forEach((partition, state) -> {
            if (state.completedOffset() >= 0) offsets.put(partition, new OffsetAndMetadata(state.completedOffset()));
        });
        return offsets;
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
        var consumer = createConsumer(topic, group, reset, 256);
        consumers.add(consumer);
        Thread thread = Thread.ofPlatform().daemon(true).name(group + "-" + threads.size()).unstarted(() -> {
            var completed = new java.util.HashMap<TopicPartition, OffsetAndMetadata>();
            try {
                consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        var offsets = new HashMap<>(completed);
                        offsets.keySet().retainAll(partitions);
                        if (!offsets.isEmpty()) {
                            try { consumer.commitSync(offsets, Duration.ofSeconds(2)); }
                            catch (RuntimeException ignored) { /* Durable inbox IDs make replay safe. */ }
                        }
                        partitions.forEach(completed::remove);
                    }
                    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
                });
                while (running) {
                    try {
                    var records = consumer.poll(Duration.ofMillis(100));
                    if (assigned != null && assigned.getCount() != 0 && !consumer.assignment().isEmpty()) {
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
                            consumer.commitAsync(completed, (offsets, failure) -> {
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

    private KafkaConsumer<String, String> createConsumer(String topic, String group, String reset, int maxRecords) {
        var config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("group.id", group);
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", reset);
        config.put("max.poll.records", Integer.toString(maxRecords));
        config.put("fetch.min.bytes", "1");
        config.put("fetch.max.wait.ms", Integer.toString(topic.endsWith(".scans") ? eventFetchWaitMs : fetchWaitMs));
        config.put("max.partition.fetch.bytes", "1048576");
        config.put("max.poll.interval.ms", "300000");
        return new KafkaConsumer<>(config);
    }

    private enum Stage {
        REQUEST_TOTAL("requestMs"), REQUEST_ACK("requestAckMs"), RECORD_AGE("recordAgeMs"),
        DISPATCH_QUEUE("dispatchQueueMs"), HANDLER("handlerMs"), REPLY_ACK("replyAckMs");
        final String label;
        Stage(String label) { this.label = label; }
    }

    /** Cumulative diagnostics only; this never changes delivery, acknowledgment or timeout behavior. */
    private static final class PerformanceLog {
        private static final Stage[] STAGES = Stage.values();
        final boolean enabled;
        private final String service;
        private final AtomicLong completed = new AtomicLong();
        private final Map<String, Samples> operations = new ConcurrentHashMap<>();

        PerformanceLog(String service, boolean enabled) { this.service = service; this.enabled = enabled; }
        long start() { return enabled ? System.nanoTime() : 0; }

        void elapsed(String operation, Stage stage, long started, boolean failed) {
            if (enabled) observe(operation, stage, System.nanoTime() - started, failed);
        }

        void observe(String operation, Stage stage, long durationNanos, boolean failed) {
            if (!enabled) return;
            var samples = operations.computeIfAbsent(operation, ignored -> new Samples());
            int index = stage.ordinal();
            samples.nanos[index].add(Math.max(0, durationNanos));
            samples.count[index].increment();
            if (failed) samples.failed[index].increment();
        }

        void completed() {
            if (!enabled) return;
            long total = completed.incrementAndGet();
            if (total % 1000 != 0) return;
            dump(total);
        }

        void flush() {
            if (enabled && completed.get() > 0) dump(completed.get());
        }

        private void dump(long total) {
            for (var entry : new java.util.TreeMap<>(operations).entrySet()) {
                var line = new StringBuilder("[KafkaPerf] service=").append(service).append(" completed=").append(total)
                        .append(" operation=").append(entry.getKey());
                for (var stage : STAGES) {
                    int index = stage.ordinal();
                    long count = entry.getValue().count[index].sum();
                    if (count == 0) continue;
                    double meanMs = entry.getValue().nanos[index].sum() / (double) count / 1_000_000;
                    line.append(' ').append(stage.label).append("={n:").append(count).append(",mean:")
                            .append(String.format(java.util.Locale.ROOT, "%.3f", meanMs)).append(",fail:")
                            .append(entry.getValue().failed[index].sum()).append('}');
                }
                System.err.println(line);
            }
        }

        private static final class Samples {
            final LongAdder[] count = adders();
            final LongAdder[] nanos = adders();
            final LongAdder[] failed = adders();
            private static LongAdder[] adders() {
                var values = new LongAdder[STAGES.length];
                java.util.Arrays.setAll(values, ignored -> new LongAdder());
                return values;
            }
        }
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
        commandExecutor.shutdown();
        try { commandExecutor.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        producer.close(Duration.ofSeconds(3));
        performance.flush();
    }
}
