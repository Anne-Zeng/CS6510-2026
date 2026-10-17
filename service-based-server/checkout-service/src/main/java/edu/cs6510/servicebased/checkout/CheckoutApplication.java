package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.shared.common.GlobalExceptionHandler;
import edu.cs6510.servicebased.shared.common.HealthController;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import edu.cs6510.servicebased.shared.persistence.CheckoutStore;
import edu.cs6510.servicebased.shared.persistence.ScanEventStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** 功能：独立启动结账/库存服务；不加载 Catalog 或 Analytics 的业务组件。 */
@SpringBootApplication
@EntityScan("edu.cs6510.servicebased.shared.persistence.entity")
@EnableJpaRepositories("edu.cs6510.servicebased.shared.persistence")
@Import({CatalogCache.class, CheckoutStore.class, ScanEventStore.class, GlobalExceptionHandler.class, HealthController.class})
public class CheckoutApplication {
    // 功能：以独立 JVM 提供交易和库存 HTTP API。
    public static void main(String[] args) {
        SpringApplication.run(CheckoutApplication.class, args);
    }
}
