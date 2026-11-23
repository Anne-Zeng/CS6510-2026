package edu.cs6510.notification;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class StatusController {
    private final NotificationStore store;
    private final SubscriberHandler handler;
    public StatusController(NotificationStore store, SubscriberHandler handler) {
        this.store = store;
        this.handler = handler;
    }
    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "UP", "service", "notification-service"); }
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        var metrics = new LinkedHashMap<>(store.metrics());
        metrics.put("connected_users", handler.connectedUsers());
        return metrics;
    }
    @GetMapping("/cache-metrics")
    public Map<String, Long> cacheMetrics() { return store.payloadCacheMetrics(); }
}
