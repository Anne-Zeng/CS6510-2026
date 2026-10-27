package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.contract.PopularItem;
import edu.cs6510.servicebased.analytics.contract.PopularItemsResponse;
import edu.cs6510.servicebased.analytics.persistence.AnalyticsWindowRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import edu.cs6510.servicebased.analytics.persistence.PopularItemResultRepository;
import edu.cs6510.servicebased.analytics.persistence.entity.AnalyticsWindow;
import edu.cs6510.servicebased.analytics.persistence.entity.PopularItemResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** 统一处理排名读写；窗口元数据和商品排名必须在同一个数据库事务中提交。 */
@Component
public class AnalyticsWindowStore {
    // 窗口表同时充当恢复检查点；排名表保存每个窗口的前十名；扫描事件提供商品名称。
    private final AnalyticsWindowRepository windows;
    private final PopularItemResultRepository rankings;
    private final JdbcTemplate jdbc;

    /** 注入本服务的 Repository 和 JDBC，不依赖其他服务的 Java 对象。 */
    public AnalyticsWindowStore(AnalyticsWindowRepository windows,
                                PopularItemResultRepository rankings,
                                JdbcTemplate jdbc) {
        this.windows = windows;
        this.rankings = rankings;
        this.jdbc = jdbc;
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
        // 一个本地事务、一次集合式 SQL 保存元数据与全部排名，重试命中窗口唯一键即不再插入。
        var top=snapshot.ranked().stream().limit(topLimit).toList();
        if(top.isEmpty()) throw new IllegalArgumentException("A complete window must have rankings");
        var values=new java.util.StringJoiner(",");
        var parameters=new ArrayList<Object>();
        parameters.add(windowSize);parameters.add(slideInterval);
        parameters.add(snapshot.firstSequence());parameters.add(snapshot.lastSequence());
        for(int index=0;index<top.size();index++) {
            values.add("(?,?,?)");
            parameters.add(top.get(index).sku());parameters.add((int)top.get(index).count());parameters.add(index+1);
        }
        parameters.add(snapshot.lastSequence());
        int inserted=jdbc.update("""
          WITH created AS (
            INSERT INTO analytics_windows(window_size,slide_interval,window_start,window_end,computed_at)
            VALUES (?,?,?,?,statement_timestamp()) ON CONFLICT(window_end) DO NOTHING RETURNING id
          ), ranked(sku,scan_count,rank) AS (VALUES %s)
          INSERT INTO popular_item_results(window_id,sku,name,scan_count,rank)
          SELECT created.id,r.sku,
            (SELECT e.name FROM scan_events e WHERE e.sku=r.sku AND e.sequence<=? ORDER BY e.sequence DESC LIMIT 1),
            r.scan_count,r.rank FROM created CROSS JOIN ranked r
          """.formatted(values),parameters.toArray());
        if(inserted!=0 && inserted!=top.size()) throw new IllegalStateException("Incomplete persisted ranking");
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
