package edu.cs6510.monolith_server.analytics;

import edu.cs6510.monolith_server.analytics.api.PopularItemsResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @GetMapping("/popular-items")
    public ResponseEntity<PopularItemsResponse> getPopularItems(
            @RequestParam(required = false) Integer limit
    ) {
        return ResponseEntity.ok(
                analyticsService.getPopularItems(limit)
        );
    }
}