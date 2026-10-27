package edu.cs6510.events;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/** Application-facing messaging boundary. Implementations must preserve these delivery semantics. */
public interface MessageBus extends AutoCloseable {
    Reply request(String service, String operation, String key, String body);
    /** Returns only after the broker acknowledges the record; this does not acknowledge business processing. */
    void publish(String topic, String key, String body);
    void serve(String service, Function<Request, Reply> handler);
    /** Offsets are committed only after the whole callback returns successfully. */
    void subscribe(String topic, String group, Consumer<List<String>> handler);
    String topic(String suffix);
    @Override default void close() {}
}
