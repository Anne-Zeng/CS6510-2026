package edu.cs6510.servicebased.analytics;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/** 功能：只扫描 Analytics 自己的组件和持久化模型，不连接其他服务数据库。 */
@SpringBootApplication
public class AnalyticsApplication {
 public static void main(String[] args) { SpringApplication.run(AnalyticsApplication.class,args); }
}
