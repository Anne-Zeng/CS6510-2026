package edu.cs6510.servicebased.checkout;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// 后台读取 Outbox，经 AnalyticsClient 发给 Kafka，收到 broker 确认后更新状态
/** At-least-once publication: acknowledge the outbox only after Kafka broker ack.
 * A crash between broker ack and the update republishes the same event UUIDs;
 * Analytics deduplicates them with its transactional inbox.
 */
@Component
public class OutboxPublisher {
 private static final Logger log=LoggerFactory.getLogger(OutboxPublisher.class);
 private final OutboxStore store;
 private final AnalyticsClient client;
 private long lastWarning;
 public OutboxPublisher(OutboxStore store,AnalyticsClient client) { this.store=store;this.client=client; }
 public int publishOnce() {
  var events=store.pending();//调用 OutboxStore.pending()line19 读取 scan_outbox 中的待发送事件，按 BATCH_SIZE 批量返回
  if(events.isEmpty()) return 0;
  client.deliver(events);//调用 AnalyticsClient.deliver()line17将扫描事件批次发给 Kafka，等待 broker 确认
  store.acknowledge(events);//再调用 OutboxStore.acknowledge()line27 将这些事件标记为已发送
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
