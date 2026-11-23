package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore.Event;
import org.springframework.stereotype.Component;
import java.util.List;
//将扫描事件批次发布到 Kafka
/** Publish durable ItemScanned batches. publish returns only after broker ack;
 * consumers commit their inbox and statistics before advancing Kafka offsets.
 */
@Component
public class AnalyticsClient {
 public record Batch(List<Event> events) {}
 private final MessageBus bus;
 public AnalyticsClient(MessageBus bus) { this.bus=bus; }
 public void deliver(List<Event> events) {//将记录转成JSON并发布, Bus.publish()等待 broker 确认
  bus.publish(bus.topic("scans"),"checkout-scans",Json.write(new Batch(events)));
 }
}
// 内部通过：
// producer.send(record, (metadata, failure) -> {
//     if (failure == null) confirmed.complete(null);
//     else confirmed.completeExceptionally(failure);
// });
// 发送成功并收到确认后，调用栈逐层返回：
// KafkaBus.publish() 返回
//     ↓
// AnalyticsClient.deliver() 返回
//     ↓
// 继续执行 OutboxPublisher.publishOnce() 的下一行