package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.common.BusinessException;
import edu.cs6510.servicebased.shared.contract.PopularItemsResponse;
import org.springframework.stereotype.Service;

import static edu.cs6510.servicebased.analytics.PipelineData.*;

/** 提供热门商品查询业务；扫描事件由 Checkout 在自己的事务中写入共享数据库。 */
@Service
public class AnalyticsService {
    // 查询已经提交的排名快照，不让 HTTP 请求等待后台计算。
    private final AnalyticsWindowStore store;

    /** 注入排名查询与持久化组件。 */
    public AnalyticsService(AnalyticsWindowStore store) {
        this.store = store;
    }

    /** 校验查询数量并读取最新快照；无历史窗口时返回空列表。 */
    public PopularItemsResponse getPopularItems(Integer requestedLimit) {
        int limit = requestedLimit == null ? TOP_LIMIT : requestedLimit;
        if (limit < 1) {
            throw new BusinessException(BusinessException.Kind.INVALID_REQUEST,
                    "INVALID_REQUEST", "limit must be greater than zero");
        }
        return store.latestRanking(limit, WINDOW_SIZE, SLIDE_INTERVAL);
    }
}
