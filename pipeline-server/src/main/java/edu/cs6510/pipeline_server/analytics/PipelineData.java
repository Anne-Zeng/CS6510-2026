package edu.cs6510.pipeline_server.analytics;

import java.util.List;

//交接数据的格式定义两个阶段之间传递的 Java 对象Defines immutable messages passed through the queues
/** Immutable messages carried by the two pipes. */
public final class PipelineData {
    public static final int WINDOW_SIZE = 1000;
    public static final int SLIDE_INTERVAL = 500;
    public static final int TOP_LIMIT = 10;
    private PipelineData() {}

    public record WindowBatch(long firstSequence, long lastSequence, List<String> skus) {
        public WindowBatch { skus = List.copyOf(skus); }
    }
    public record SkuCount(String sku, long count) {}
    public record RankedWindow(long firstSequence, long lastSequence, List<SkuCount> ranked) {
        public RankedWindow { ranked = List.copyOf(ranked); }
    }
}
