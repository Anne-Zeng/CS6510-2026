package edu.cs6510.notification;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

final class SaleItemCache {
    record Item(long itemId, String eventId, String transactionId, String sku, int quantity, Instant completedAt) {}
    private final int capacity;
    private final LinkedHashMap<Long, Item> items = new LinkedHashMap<>(128, 0.75f, true);
    private long hits;
    private long misses;
    private long evictions;

    SaleItemCache(int capacity) {
        if (capacity < 0 || capacity > 65536) throw new IllegalArgumentException("Payload cache size must be between 0 and 65536");
        this.capacity = capacity;
    }
    boolean enabled() { return capacity > 0; }
    synchronized Item get(long itemId) {
        var item = items.get(itemId);
        if (item == null) misses++; else hits++;
        return item;
    }
    synchronized void put(Item item) {
        if (!enabled()) return;
        items.put(item.itemId(), item);
        while (items.size() > capacity) {
            items.remove(items.keySet().iterator().next());
            evictions++;
        }
    }
    synchronized Map<String, Long> metrics() {
        return Map.of("capacity", (long) capacity, "size", (long) items.size(), "hits", hits,
                "misses", misses, "evictions", evictions);
    }
}
