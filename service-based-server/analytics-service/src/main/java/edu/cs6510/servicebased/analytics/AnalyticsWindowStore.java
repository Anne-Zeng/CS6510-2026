package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.contract.PopularItem;
import edu.cs6510.servicebased.shared.contract.PopularItemsResponse;
import edu.cs6510.servicebased.shared.persistence.AnalyticsWindowRepository;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import edu.cs6510.servicebased.shared.persistence.PopularItemResultRepository;
import edu.cs6510.servicebased.shared.persistence.entity.AnalyticsWindow;
import edu.cs6510.servicebased.shared.persistence.entity.PopularItemResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** 统一处理排名读写；窗口元数据和商品排名必须在同一个数据库事务中提交。 */
@Component
public class AnalyticsWindowStore {
    // 窗口表同时充当恢复检查点；排名表保存每个窗口的前十名；缓存提供商品名称。
    private final AnalyticsWindowRepository windows;
    private final PopularItemResultRepository rankings;
    private final CatalogCache catalog;

    /** 注入共享 Repository 和只读商品缓存，不依赖其他服务的 Java 对象。 */
    public AnalyticsWindowStore(AnalyticsWindowRepository windows,
                                PopularItemResultRepository rankings,
                                CatalogCache catalog) {
        this.windows = windows;
        this.rankings = rankings;
        this.catalog = catalog;
    }

    /** 返回已成功提交的最后窗口边界；服务重启后从其下一个窗口继续。 */
    @Transactional(readOnly = true)
    public long lastPublishedSequence() {
        return windows.findTopByOrderByWindowEndDesc()
                .map(AnalyticsWindow::getWindowEnd)
                .orElse(0L);
    }

    /** 原子保存窗口和排名；同一窗口重试时直接返回，避免重复发布。 */
    @Transactional
    public void save(PipelineData.RankedWindow snapshot, int windowSize, int slideInterval, int topLimit) {
        // 提交成功后若线程被中断，重试仍能识别已存在的窗口，实现幂等写入。
        if (windows.existsByWindowEnd(snapshot.lastSequence())) return;
        AnalyticsWindow window = windows.save(new AnalyticsWindow(
                windowSize, slideInterval,
                snapshot.firstSequence(), snapshot.lastSequence(), Instant.now()));

        // 每条排名补齐商品名称；合法扫描必须引用已有商品，缺失表示数据损坏。
        List<PipelineData.SkuCount> top = snapshot.ranked().stream().limit(topLimit).toList();
        List<PopularItemResult> results = new ArrayList<>(top.size());
        for (int index = 0; index < top.size(); index++) {
            PipelineData.SkuCount entry = top.get(index);
            String name = catalog.find(entry.sku())
                    .orElseThrow(() -> new IllegalStateException("Uncatalogued SKU in window: " + entry.sku()))
                    .getName();
            results.add(new PopularItemResult(window, entry.sku(), name, (int) entry.count(), index + 1));
        }
        rankings.saveAll(results);
    }

    /** 读取最近的完整窗口；尚未积累足够扫描记录时返回空的合法响应。 */
    @Transactional(readOnly = true)
    public PopularItemsResponse latestRanking(int limit, int windowSize, int slideInterval) {
        Optional<AnalyticsWindow> latest = windows.findTopByOrderByWindowEndDesc();
        if (latest.isEmpty()) {
            return new PopularItemsResponse(windowSize, slideInterval, 0, 0, Instant.now(), List.of());
        }

        // 数据库最多保存前十名；limit 更小时取前缀，更大时仍只返回已保存结果。
        AnalyticsWindow window = latest.get();
        List<PopularItem> items = rankings.findAllByWindowIdOrderByRankAsc(window.getId()).stream()
                .limit(limit)
                .map(row -> new PopularItem(row.getSku(), row.getName(), row.getScanCount(), row.getRank()))
                .toList();
        return new PopularItemsResponse(window.getWindowSize(), window.getSlideInterval(),
                window.getWindowStart(), window.getWindowEnd(), window.getComputedAt(), items);
    }
}
