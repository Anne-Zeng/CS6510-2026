package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.contract.PopularItemsResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 保留原负载客户端的 URL 与 JSON 契约，让代理能够透明转发查询。 */
@RestController
@RequestMapping("/analytics")
public class AnalyticsController {
    // 控制器只处理 HTTP 参数，业务校验交给 AnalyticsService。
    private final AnalyticsService analytics;

    /** 注入本进程内的业务对象，不建立服务间网络调用。 */
    public AnalyticsController(AnalyticsService analytics) {
        this.analytics = analytics;
    }

    /** GET /analytics/popular-items?limit=10 返回最近已保存窗口的热门商品。 */
    @GetMapping("/popular-items")
    public PopularItemsResponse getPopularItems(@RequestParam(required = false) Integer limit) {
        return analytics.getPopularItems(limit);
    }
}
