package edu.cs6510.notification;

import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Json;
import edu.cs6510.events.SaleCompleted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
// Notification后台订阅 Checkout 发布的销售事件，按 SKU 匹配 Notification Service 的订阅者，保存待投递通知到数据库
//line37 启动时注册 Kafka 消费者，接收销售事件，按 SKU 匹配订阅者，保存待投递通知到数据库
@Component
public class SalesEvents {
    private static final Logger log = LoggerFactory.getLogger(SalesEvents.class);
    private final MessageBus bus;
    private final NotificationStore store;
    private final boolean stageTiming;
    private final int batchSize;
    public SalesEvents(MessageBus bus, NotificationStore store,
            @Value("${notification.stage-timing:false}") boolean stageTiming,
            @Value("${notification.ingest-batch-size:16}") int batchSize) {
        if (batchSize < 1 || batchSize > 256)
            throw new IllegalArgumentException("Ingest batch size must be between 1 and 256");
        this.bus = bus;
        this.store = store;
        this.stageTiming = stageTiming;
        this.batchSize = batchSize;
    }
    @EventListener(ApplicationReadyEvent.class)
    public void start() {//消费 sales topic，使用 notification-sales consumer group， 将收到的消息交给 accept(...)
        bus.subscribe(bus.topic("sales"), "notification-sales", this::accept);
    }//accept(...) 将记录分批传给 store.accept(chunk);默认每批最多 16 笔销售事件。这个参数控制通知数据库的单次处理规模
    void accept(List<String> records) {
        Instant received = stageTiming ? Instant.now() : null;
        for (int offset = 0; offset < records.size(); offset += batchSize) {
            var chunk = List.copyOf(records.subList(offset, Math.min(offset + batchSize, records.size())));
            Instant started = stageTiming ? Instant.now() : null;
            store.accept(chunk);//传入NotificationStore去重并生成通知
            if (stageTiming) {
                Instant committed = Instant.now();
                var eventIds = chunk.stream().map(body -> Json.read(body, SaleCompleted.class).eventId()).toList();
                log.info("NOTIFICATION_INGEST_TIMING {}", Json.write(Map.of(
                        "callbackReceivedAt", received, "batchStartedAt", started,
                        "commitObservedAt", committed, "eventIds", eventIds)));
            }
        }
    }
}
