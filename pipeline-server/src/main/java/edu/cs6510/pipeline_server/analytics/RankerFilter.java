package edu.cs6510.pipeline_server.analytics;

import org.springframework.stereotype.Component;

import edu.cs6510.pipeline_server.analytics.PipelineData.RankedWindow;
import edu.cs6510.pipeline_server.analytics.PipelineData.SkuCount;
import edu.cs6510.pipeline_server.analytics.PipelineData.WindowBatch;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

//第二位工作人员统计每个 SKU 的次数，排序，取前十名Counts scans per SKU, sorts them, and selects the top ten
/** Stage 2: pure calculation; no database access. */
@Component
public class RankerFilter {
    public RankedWindow rank(WindowBatch batch) {
        var counts = batch.skus().stream()
                .collect(Collectors.groupingBy(sku -> sku, Collectors.counting()));
        var top = counts.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(TOP_LIMIT)
                .map(e -> new SkuCount(e.getKey(), e.getValue())).toList();
        return new RankedWindow(batch.firstSequence(), batch.lastSequence(), top);
    }
}
