package edu.cs6510.notification;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SaleItemCacheTest {
    private SaleItemCache.Item item(long identifier) {
        return new SaleItemCache.Item(identifier, "event", "transaction", "sku", 3, Instant.EPOCH);
    }
    @Test void evictsLeastRecentlyUsedAndTracksCounters() {
        var cache = new SaleItemCache(2);
        cache.put(item(1));
        cache.put(item(2));
        assertEquals(item(1), cache.get(1));
        cache.put(item(3));
        assertNull(cache.get(2));
        assertEquals(2L, cache.metrics().get("size"));
        assertEquals(1L, cache.metrics().get("evictions"));
        assertEquals(1L, cache.metrics().get("hits"));
        assertEquals(1L, cache.metrics().get("misses"));
    }
    @Test void zeroDisablesStorageAndInvalidCapacitiesFail() {
        var cache = new SaleItemCache(0);
        cache.put(item(1));
        assertFalse(cache.enabled());
        assertNull(cache.get(1));
        assertEquals(0L, cache.metrics().get("size"));
        assertThrows(IllegalArgumentException.class, () -> new SaleItemCache(-1));
        assertThrows(IllegalArgumentException.class, () -> new SaleItemCache(65537));
    }
    @Test void concurrentAccessRemainsBounded() {
        var cache = new SaleItemCache(32);
        java.util.stream.LongStream.range(0, 10000).parallel().forEach(identifier -> {
            cache.put(item(identifier));
            cache.get(identifier);
        });
        assertEquals(32L, cache.metrics().get("size"));
        assertEquals(9968L, cache.metrics().get("evictions"));
    }
}
