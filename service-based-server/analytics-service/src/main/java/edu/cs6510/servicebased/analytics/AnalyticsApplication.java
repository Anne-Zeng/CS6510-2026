package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.common.GlobalExceptionHandler;
import edu.cs6510.servicebased.shared.common.HealthController;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import edu.cs6510.servicebased.shared.persistence.ScanEventStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** 热门商品分析的独立进程；仅扫描本服务的业务组件，显式加载共享数据库组件。 */
@SpringBootApplication(scanBasePackages = "edu.cs6510.servicebased.analytics")
@EntityScan("edu.cs6510.servicebased.shared.persistence.entity")
@EnableJpaRepositories("edu.cs6510.servicebased.shared.persistence")
@Import({ScanEventStore.class, CatalogCache.class, GlobalExceptionHandler.class, HealthController.class})
public class AnalyticsApplication {
    /** 启动独立 HTTP 服务和后台分析线程，不负责建表或初始化商品。 */
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}
