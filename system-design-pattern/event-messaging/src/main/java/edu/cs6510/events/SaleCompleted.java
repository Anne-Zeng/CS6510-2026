package edu.cs6510.events;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
//销售完成事件，Checkout 生成并发布给 Notification Service、
//Analytics Service、Accounting Service 等，通知库存扣减、销售统计、财务入账等
//定义事件的结构和校验规则，包括：
// - schemaVersion
// - eventId
// - transactionId
// - completedAt
// - items
// 注意：生产端在 SQL 中构造 JSON；这个 Java record 用于表达和解析同一事件格式 next 后台 Publisher：把销售事件推送到 Kafka，checkout/SaleOutboxPublisher.java
/** SaleCompleted is a business event published by the Checkout Service when a sale is completed. */
public record SaleCompleted(int schemaVersion, String eventId, String transactionId,
                            Instant completedAt, List<Item> items) {
    public record Item(String sku, int quantity) {
        public Item {
            if (sku == null || sku.isBlank() || sku.length() > 32 || quantity < 1)
                throw new IllegalArgumentException("Invalid sale item");
        }
    }
    public SaleCompleted {
        if (schemaVersion != 1 || completedAt == null || items == null || items.isEmpty())
            throw new IllegalArgumentException("Invalid sale event");
        UUID.fromString(eventId);
        UUID.fromString(transactionId);
        items = List.copyOf(items);
        var seen = new HashSet<String>();
        for (var item : items)
            if (!seen.add(item.sku())) throw new IllegalArgumentException("Duplicate SKU in sale");
    }
}
