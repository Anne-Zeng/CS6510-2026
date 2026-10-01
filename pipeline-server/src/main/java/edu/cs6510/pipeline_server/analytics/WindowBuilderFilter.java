package edu.cs6510.pipeline_server.analytics;

import edu.cs6510.pipeline_server.analytics.PipelineData.WindowBatch;
import edu.cs6510.pipeline_server.persistence.ScanEventStore;
import org.springframework.stereotype.Component;
import java.util.Optional;
import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

//第一位工作人员从数据库读取一个准确窗口的扫描记录Builds exact windows of up to 1,000 scans, advancing every 500
/** Stage 1: reads an exact window from the committed, immutable scan log. */
@Component
public class WindowBuilderFilter {
    private final ScanEventStore events;
    public WindowBuilderFilter(ScanEventStore events) { this.events = events; }

    public Optional<WindowBatch> build(long end) {
        if (end < SLIDE_INTERVAL || end % SLIDE_INTERVAL != 0) {
            throw new IllegalArgumentException("Window end must be a positive slide boundary");
        }
        if (events.count() < end) return Optional.empty();
        long start = Math.max(1, end - WINDOW_SIZE + 1);
        var skus = events.between(start, end);
        if (skus.size() != end - start + 1) {
            throw new IllegalStateException("Incomplete event log for window " + end);
        }
        return Optional.of(new WindowBatch(start, end, skus));
    }
}
