package edu.cs6510.servicebased.inventory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/** 功能：独立启动库存服务，只连接 inventory_db。 */
@SpringBootApplication
public class InventoryApplication {
 public static void main(String[] args) { SpringApplication.run(InventoryApplication.class, args); }
}
