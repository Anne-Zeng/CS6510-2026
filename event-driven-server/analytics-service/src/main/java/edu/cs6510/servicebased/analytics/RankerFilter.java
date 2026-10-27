package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.PipelineData.RankedWindow;
import edu.cs6510.servicebased.analytics.PipelineData.SkuCount;
import edu.cs6510.servicebased.analytics.PipelineData.WindowBatch;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import static edu.cs6510.servicebased.analytics.PipelineData.TOP_LIMIT;

/** 第二段：纯内存计算热门商品，不读取或更新数据库。 */
@Component
public class RankerFilter {
    /** 按扫描次数降序排名；次数相同时按 SKU 升序，保证每次计算结果一致。 */
    public RankedWindow rank(WindowBatch batch) {
        // 将窗口内重复出现的 SKU 汇总为“SKU → 扫描次数”。
        var counts = batch.skus().stream()
                .collect(Collectors.groupingBy(sku -> sku, Collectors.counting()));
        var top = counts.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(TOP_LIMIT)
                .map(entry -> new SkuCount(entry.getKey(), entry.getValue()))
                .toList();
        return new RankedWindow(batch.firstSequence(), batch.lastSequence(), top);
    }
}
