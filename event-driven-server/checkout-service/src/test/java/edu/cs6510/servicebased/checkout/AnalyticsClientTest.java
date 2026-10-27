package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore.Event;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AnalyticsClientTest {
 MessageBus bus=mock(MessageBus.class);
 AnalyticsClient client=new AnalyticsClient(bus);
 List<Event> events=List.of(new Event("event-1","A","Apple"));
 @Test void preservesEventIdentityInPublishedBatch() {
  when(bus.topic("scans")).thenReturn("test.scans");
  client.deliver(events);
  var body=org.mockito.ArgumentCaptor.forClass(String.class);
  verify(bus).publish(eq("test.scans"),eq("checkout-scans"),body.capture());
  assertEquals(events,Json.read(body.getValue(),AnalyticsClient.Batch.class).events());
 }
 @Test void missingBrokerAcknowledgementMustReachOutboxPublisher() {
  when(bus.topic("scans")).thenReturn("test.scans");
  doThrow(new IllegalStateException("Kafka acknowledgement lost"))
   .when(bus).publish(eq("test.scans"),eq("checkout-scans"),anyString());
  assertThrows(IllegalStateException.class,()->client.deliver(events));
 }
}
