package edu.cs6510.pipeline_server.api;

import edu.cs6510.pipeline_server.analytics.AnalyticsService;
import edu.cs6510.pipeline_server.contract.PopularItemsResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/analytics")
public class AnalyticsController {
    private final AnalyticsService analytics;
    public AnalyticsController(AnalyticsService analytics) { this.analytics = analytics; }
    @GetMapping("/popular-items")
    public PopularItemsResponse getPopularItems(@RequestParam(required = false) Integer limit) {
        return analytics.getPopularItems(limit);
    }
}
