package edu.cs6510.servicebased.checkout;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore.Event;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import java.net.http.HttpClient;
import java.time.Duration;
import jakarta.annotation.PreDestroy;
import java.util.List;

/** 功能：批量投递；只有接收方明确确认了所有 ID，才允许标记已投递。 */
@Component
public class AnalyticsClient {
 public record Batch(List<Event> events) {}
 public record Ack(List<String> acceptedEventIds) {}
 private final RestClient client;
 private final HttpClient http;
 public AnalyticsClient(@Value("${analytics.base-url:http://localhost:8083}") String url) {
  http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).version(HttpClient.Version.HTTP_1_1).build();
  var factory=new JdkClientHttpRequestFactory(http);factory.setReadTimeout(Duration.ofSeconds(5));
  client=RestClient.builder().baseUrl(url).requestFactory(factory).build();
 }
 @PreDestroy public void close() { http.close(); }
 public void deliver(List<Event> events) {
  var ack=client.post().uri("/internal/events/scans").body(new Batch(events)).retrieve().body(Ack.class);
  if(ack==null || !events.stream().map(Event::eventId).toList().equals(ack.acceptedEventIds()))
   throw new IllegalStateException("Incomplete analytics acknowledgement");
 }
}
