package edu.cs6510.layered_server.persistence;

import edu.cs6510.layered_server.persistence.entity.CatalogItem;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-through cache over {@link CatalogItemRepository}, living in the database
 * access layer so that both the transactions layer and the analytics layer get
 * catalog lookups without each keeping its own copy.
 *
 * <p>The catalog is written once by the seeder and never modified afterwards, so
 * a snapshot cannot go stale. Caching it removes one database round-trip from
 * every scan, which is the highest-volume operation in the system.
 */
@Component
public class CatalogCache {

    private final CatalogItemRepository catalog;
    private volatile Map<String, CatalogItem> bySku = Map.of();

    public CatalogCache(CatalogItemRepository catalog) {
        this.catalog = catalog;
    }

    public Optional<CatalogItem> find(String sku) {
        return sku == null ? Optional.empty() : Optional.ofNullable(snapshot().get(sku));
    }

    public List<CatalogItem> findAllOrderedBySku() {
        return snapshot().values().stream()
                .sorted(Comparator.comparing(CatalogItem::getSku))
                .toList();
    }

    /**
     * Warms the cache once the seeder has run. {@link ApplicationReadyEvent}
     * fires after every {@code CommandLineRunner} has finished, so the catalog
     * is guaranteed to be fully seeded by this point.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warm() {
        reload();
    }

    private Map<String, CatalogItem> snapshot() {
        Map<String, CatalogItem> loaded = bySku;
        // An empty catalog is never a valid serving state, so it is treated as
        // "not loaded yet" rather than cached. Tomcat accepts requests before the
        // seeder finishes, and memoising that empty result would strand the
        // server permanently serving an empty catalog.
        if (loaded.isEmpty()) {
            loaded = reload();
        }
        return loaded;
    }

    private synchronized Map<String, CatalogItem> reload() {
        // Re-check inside the lock so concurrent first requests load only once.
        if (!bySku.isEmpty()) {
            return bySku;
        }
        // Detached copies: callers only read sku/name/price, and CatalogItem has
        // no lazy associations, so using them outside a transaction is safe.
        Map<String, CatalogItem> loaded = catalog.findAll().stream()
                .collect(Collectors.toUnmodifiableMap(CatalogItem::getSku, Function.identity()));
        bySku = loaded;
        return loaded;
    }
}
