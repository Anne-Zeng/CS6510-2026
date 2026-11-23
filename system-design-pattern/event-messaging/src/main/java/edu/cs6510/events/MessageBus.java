package edu.cs6510.events;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
//interface通信接口，业务代码通过它发送和接收消息
/** Application-facing messaging boundary. Implementations must preserve these delivery semantics. */
public interface MessageBus extends AutoCloseable {
    //发请求，等待业务响应，Checkout 要求 Inventory 扣库存
    Reply request(String service, String operation, String key, String body);
    /** Returns only after the broker acknowledges the record; this does not acknowledge business processing. */
    //发布消息，等待 Kafka 确认接收，Checkout 发布扫描事件
    void publish(String topic, String key, String body);
    //注册请求处理函数，Checkout 接收 SCAN_ITEM
    void serve(String service, Function<Request, Reply> handler);
    /** Offsets are committed only after the whole callback returns successfully. */
    //订阅事件，Analytics 消费扫描事件
    void subscribe(String topic, String group, Consumer<List<String>> handler);
    String topic(String suffix);
    @Override default void close() {}
}
