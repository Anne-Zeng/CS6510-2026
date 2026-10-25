package edu.cs6510.servicebased.checkout;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
/** 功能：只扫描 Checkout 自己的组件，不加载共享实体和 Repository。 */
@SpringBootApplication
@EnableScheduling
public class CheckoutApplication {
 public static void main(String[] args) { SpringApplication.run(CheckoutApplication.class,args); }
}
