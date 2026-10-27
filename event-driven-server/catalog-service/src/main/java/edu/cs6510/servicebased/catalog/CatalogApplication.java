package edu.cs6510.servicebased.catalog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import edu.cs6510.events.MessagingConfiguration;
/** 启动 Spring Boot 应用,读取properties, 注册本服务及其子包中的组件：扫描 Catalog 自己的组件、实体和 Repository。 */
@SpringBootApplication
@Import(MessagingConfiguration.class)
public class CatalogApplication {
    //SpringApplication.run创建 Spring 容器（创建对象controller service, Repository 接口由 Spring Data 生成实现对象）、
    // 加载配置（application.properties或application.yml）、注册组件（bean,）、完成依赖注入；如果项目包含 Web 依赖，还会启动内嵌 Web 服务器。
    public static void main(String[] args) { SpringApplication.run(CatalogApplication.class, args); }
}
