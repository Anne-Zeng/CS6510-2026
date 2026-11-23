package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.MessageBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.sql.Timestamp;
//后台读取 sale_outbox表，向消息总线event message发布销售事件，并标记已处理processed。
//Kafka sales topic > Notification Service > 按 SKU 匹配订阅 > 保存待投递通知 > 发送邮件通知 > 各订阅者的 WebSocket > subscriber-client：记录日志、发送 ACK
@Component
public class SaleOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(SaleOutboxPublisher.class);
    private final JdbcTemplate jdbc;
    private final MessageBus bus;
    public SaleOutboxPublisher(JdbcTemplate jdbc, MessageBus bus) {
        this.jdbc = jdbc;
        this.bus = bus;
    }
    public int publishOnce() {
        var records = jdbc.query(//读取待发布事件，100 是每次查询的 outbox 数量上限，不是订阅者数量
                "SELECT transaction_id,payload::text FROM sale_outbox WHERE NOT processed ORDER BY created_at LIMIT 100",
                (row, index) -> new String[]{row.getString(1), row.getString(2)});
        for (var record : records) {
            Instant publishStarted = Instant.now();
            //发布到 sales topic， publish(...) 等待 Kafka producer 的发送结果。这里确认的是 broker 接收，不是订阅者已经收到
            bus.publish(bus.topic("sales"), record[0], record[1]);
            Instant brokerAcked = Instant.now();
            //发布成功后标记 processed
            jdbc.update("""
                UPDATE sale_outbox SET processed=true,
                publish_started_at=COALESCE(publish_started_at,?),
                broker_acked_at=COALESCE(broker_acked_at,?)
                WHERE transaction_id=?
                """, Timestamp.from(publishStarted), Timestamp.from(brokerAcked), record[0]);
        }
        return records.size();
    }
    @Scheduled(fixedDelayString = "${checkout.sales.poll-ms:100}", scheduler = "salesScheduler")
    public void publish() {
        try {
            publishOnce();
        } catch (Exception failure) {
            log.warn("Sales publication failed; durable outbox retained: {}", failure.toString());
        }
    }
}
