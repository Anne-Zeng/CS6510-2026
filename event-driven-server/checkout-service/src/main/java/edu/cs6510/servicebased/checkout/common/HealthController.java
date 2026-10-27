package edu.cs6510.servicebased.checkout.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

/** 功能：各进程独立提供健康检查；数据库不可用时不会错误返回健康。 */
@RestController
public class HealthController {
    // 功能：记录服务名称并持有本进程的数据库连接入口。
    private final JdbcTemplate jdbc;
    private final String service;

    // 功能：由 Spring 注入连接和当前应用名称。
    public HealthController(JdbcTemplate jdbc, @Value("${spring.application.name}") String service) {
        this.jdbc = jdbc;
        this.service = service;
    }

    // 功能：执行轻量 SELECT，确认本服务能够访问Checkout 私有数据库。
    @GetMapping("/health")
    public Map<String, String> health() {
        jdbc.queryForObject("SELECT 1", Integer.class);
        return Map.of("status", "UP", "service", service);
    }
}
