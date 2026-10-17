package edu.cs6510.servicebased.shared.persistence;

import edu.cs6510.servicebased.shared.persistence.entity.CatalogItem;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** 功能：每个进程各自缓存不可变目录；缓存数据来源是同一个集中数据库。 */

@Component
public class CatalogCache {

    private final CatalogItemRepository catalog;
    private volatile Map<String, CatalogItem> bySku = Map.of();

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public CatalogCache(CatalogItemRepository catalog) {
        this.catalog = catalog;
    }

    // 功能：从目录快照查找 SKU；空输入或不存在时返回 Optional.empty。
    public Optional<CatalogItem> find(String sku) {
        return sku == null ? Optional.empty() : Optional.ofNullable(snapshot().get(sku));
    }

    // 功能：以确定的 SKU 顺序返回完整目录，保持客户端采样输入稳定。
    public List<CatalogItem> findAllOrderedBySku() {
        return snapshot().values().stream()
                .sorted(Comparator.comparing(CatalogItem::getSku))
                .toList();
    }


    @EventListener(ApplicationReadyEvent.class)
    // 功能：应用就绪后加载目录；数据必须由初始化脚本提前准备。
    public void warm() {
        reload();
    }

    // 功能：读取已加载快照；第一次请求与启动预热并发时也能安全加载。
    private Map<String, CatalogItem> snapshot() {
        Map<String, CatalogItem> loaded = bySku;

        if (loaded.isEmpty()) {
            loaded = reload();
        }
        return loaded;
    }

    // 功能：同步加载一次目录；发布不可变 Map，供多个请求线程共享读取。
    private synchronized Map<String, CatalogItem> reload() {

        if (!bySku.isEmpty()) {
            return bySku;
        }

        // 功能：目录在初始化后固定；每个进程读取一次，商品变更需要重启相关服务刷新快照。
        Map<String, CatalogItem> loaded = catalog.findAll().stream()
                .collect(Collectors.toUnmodifiableMap(CatalogItem::getSku, Function.identity()));
        bySku = loaded;
        return loaded;
    }
}
