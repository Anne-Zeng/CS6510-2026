package edu.cs6510.servicebased.inventory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import edu.cs6510.events.MessagingConfiguration;
/** 功能：独立启动库存服务，只连接 inventory_db。 */
@SpringBootApplication
@Import(MessagingConfiguration.class)
public class InventoryApplication {
 public static void main(String[] args) { SpringApplication.run(InventoryApplication.class, args); }
}
