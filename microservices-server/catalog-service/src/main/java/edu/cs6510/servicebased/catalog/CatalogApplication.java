package edu.cs6510.servicebased.catalog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/** 功能：仅扫描 Catalog 自己的组件、实体和 Repository。 */
@SpringBootApplication
public class CatalogApplication {
    public static void main(String[] args) { SpringApplication.run(CatalogApplication.class, args); }
}
