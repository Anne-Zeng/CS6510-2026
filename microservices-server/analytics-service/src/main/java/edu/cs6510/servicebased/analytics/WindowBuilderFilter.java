package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.PipelineData.WindowBatch;
import edu.cs6510.servicebased.analytics.persistence.ScanEventStore;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static edu.cs6510.servicebased.analytics.PipelineData.*;

/** 第一段：批量整理本服务已接收的 inbox，再从连续扫描日志构造精确窗口。 */
@Component
public class WindowBuilderFilter {
    // 持久化组件封装 SQL；本服务不调用 Checkout 的 HTTP API。
    private final ScanEventStore events;

    /** 注入本地扫描日志的数据库访问组件。 */
    public WindowBuilderFilter(ScanEventStore events) {
        this.events = events;
    }

    /** 数据足够时返回窗口；不足时让后台线程稍后重试，不发布不完整结果。 */
    public Optional<WindowBatch> build(long end) {
        if (end < SLIDE_INTERVAL || end % SLIDE_INTERVAL != 0) {
            throw new IllegalArgumentException("Window end must be a positive slide boundary");
        }
        // 无完整窗口时仍整理 inbox：不足 500 条的最后一批也必须持久化到连续日志。
        // 批量事务避免每次客户端扫描都争用全局序号锁；窗口发布仍严格等待完整边界。
        if (events.count() < end) {
            events.promotePending();
            if (events.count() < end) return Optional.empty();
        }

        // 最早的窗口允许少于 1,000 条；后续窗口保留最近 1,000 条并重叠 500 条。
        long start = Math.max(1, end - WINDOW_SIZE + 1);
        var skus = events.between(start, end);
        if (skus.size() != end - start + 1) {
            throw new IllegalStateException("Incomplete event log for window " + end);
        }
        return Optional.of(new WindowBatch(start, end, skus));
    }
}
