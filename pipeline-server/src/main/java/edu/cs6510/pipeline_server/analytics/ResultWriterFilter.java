package edu.cs6510.pipeline_server.analytics;

import org.springframework.stereotype.Component;

import edu.cs6510.pipeline_server.analytics.PipelineData.RankedWindow;

import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

//第三名工作人员把排名交给存储组件保存Sends computed results to the transactional store
/** Stage 3: delegates atomic, idempotent output to the transactional store. */
@Component
public class ResultWriterFilter {
    private final AnalyticsWindowStore store;
    public ResultWriterFilter(AnalyticsWindowStore store) { this.store = store; }
    public void write(RankedWindow window) {
        store.save(window, WINDOW_SIZE, SLIDE_INTERVAL, TOP_LIMIT);
    }
}
