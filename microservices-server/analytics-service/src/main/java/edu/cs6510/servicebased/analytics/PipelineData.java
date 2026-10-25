package edu.cs6510.servicebased.analytics;

import java.util.List;

/** 定义本服务内部流水线传递的不可变数据；这些对象不会发送给其他服务。 */
public final class PipelineData {
    // 每 500 次扫描生成一个最多包含最近 1,000 次扫描的窗口，并保存前十名。
    public static final int WINDOW_SIZE = 1000;
    public static final int SLIDE_INTERVAL = 500;
    public static final int TOP_LIMIT = 10;

    /** 工具类只承载常量和消息类型，无需创建实例。 */
    private PipelineData() {}

    /** 第一段输出：窗口边界和按扫描顺序排列的 SKU。 */
    public record WindowBatch(long firstSequence, long lastSequence, List<String> skus) {
        /** 复制列表，避免生产者或消费者修改其他线程正在读取的数据。 */
        public WindowBatch { skus = List.copyOf(skus); }
    }

    /** 单个商品在当前窗口内的扫描次数。 */
    public record SkuCount(String sku, long count) {}

    /** 第二段输出：带窗口边界的排序结果，交给写入阶段保存。 */
    public record RankedWindow(long firstSequence, long lastSequence, List<SkuCount> ranked) {
        /** 复制排名列表，保证跨线程交接后的内容保持不变。 */
        public RankedWindow { ranked = List.copyOf(ranked); }
    }
}
