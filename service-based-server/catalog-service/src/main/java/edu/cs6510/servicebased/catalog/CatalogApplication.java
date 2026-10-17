package edu.cs6510.servicebased.catalog;

import edu.cs6510.servicebased.shared.common.GlobalExceptionHandler;
import edu.cs6510.servicebased.shared.common.HealthController;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** 功能：启动独立的商品目录服务，只注册本服务业务组件和必要的共享数据库组件。 */
@SpringBootApplication(scanBasePackages = "edu.cs6510.servicebased.catalog")
@EntityScan("edu.cs6510.servicebased.shared.persistence.entity")
@EnableJpaRepositories("edu.cs6510.servicebased.shared.persistence")
@Import({GlobalExceptionHandler.class, HealthController.class, CatalogCache.class})
public class CatalogApplication {
    /** 功能：提供 java -jar 的独立进程入口。 */
    public static void main(String[] args) {
        SpringApplication.run(CatalogApplication.class, args);
    }
}
