package edu.cs6510.pipeline_server.analytics;

import edu.cs6510.pipeline_server.common.BusinessException;
import edu.cs6510.pipeline_server.contract.PopularItemsResponse;
import edu.cs6510.pipeline_server.persistence.ScanEventStore;
import org.springframework.stereotype.Service;
import static edu.cs6510.pipeline_server.analytics.PipelineData.*;

//对外入口：接收扫描事件；查询已经保存的热门商品结果Records scan events and serves saved rankings to the API
/** API-facing analytics service. Scan recording stays in the checkout transaction. */
@Service
public class AnalyticsService {
    private final ScanEventStore events;
    private final AnalyticsWindowStore store;
    public AnalyticsService(ScanEventStore events, AnalyticsWindowStore store) {
        this.events = events;
        this.store = store;
    }
    public void recordScan(String sku) { events.append(sku); }
    public PopularItemsResponse getPopularItems(Integer requestedLimit) {
        int limit = requestedLimit == null ? TOP_LIMIT : requestedLimit;
        if (limit < 1) throw new BusinessException(BusinessException.Kind.INVALID_REQUEST,
                "INVALID_REQUEST", "limit must be greater than zero");
        return store.latestRanking(limit, WINDOW_SIZE, SLIDE_INTERVAL);
    }
}
