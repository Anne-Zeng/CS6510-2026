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
 public void deliver(List<Event> events) {
  bus.publish(bus.topic("scans"),"checkout-scans",Json.write(new Batch(events)));
 }
}
