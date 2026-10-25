package edu.cs6510.servicebased.checkout;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 功能：至少一次投递；接收成功后崩溃或响应丢失时会重发，Analytics 通过事件 UUID 去重。 */
@Component
public class OutboxPublisher {
 private static final Logger log=LoggerFactory.getLogger(OutboxPublisher.class);
 private final OutboxStore store;
 private final AnalyticsClient client;
 private long lastWarning;
 public OutboxPublisher(OutboxStore store,AnalyticsClient client) { this.store=store;this.client=client; }
 public int publishOnce() {
  var events=store.pending();
  if(events.isEmpty()) return 0;
  client.deliver(events);
  store.acknowledge(events);
  return events.size();
 }
 @Scheduled(fixedDelayString="${checkout.outbox.poll-ms:250}",scheduler="outboxScheduler")
 public void publish() {
  try {
   // 功能：有积压时连续发送最多十批，空闲时仍等待轮询，避免固定 400 events/s 上限。
   for(int batch=0;batch<10;batch++) if(publishOnce()<OutboxStore.BATCH_SIZE) break;
  }
  catch(Exception ex) {
   long now=System.currentTimeMillis();
   if(now-lastWarning>=10000) { lastWarning=now;log.warn("Analytics delivery pending; outbox retained: {}",ex.toString()); }
  }
 }
}
