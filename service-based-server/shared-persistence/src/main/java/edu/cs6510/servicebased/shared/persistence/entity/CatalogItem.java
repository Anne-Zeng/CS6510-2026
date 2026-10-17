package edu.cs6510.servicebased.shared.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

/** 功能：映射 catalog_items 商品表；初始化后仅供读取，各进程可缓存。 */

@Entity
@Table(name = "catalog_items")
public class CatalogItem {

    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Id
    @Column(nullable = false, updatable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal price;

    // 功能：构造器：供 JPA 反射创建实体。
    protected CatalogItem() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public CatalogItem(String sku, String name, BigDecimal price) {
        this.sku = sku;
        this.name = name;
        this.price = price;
    }

    // 功能：只读访问：返回商品 SKU。
    public String getSku() {
        return sku;
    }

    // 功能：只读访问：返回商品名称。
    public String getName() {
        return name;
    }

    // 功能：只读访问：返回目录单价。
    public BigDecimal getPrice() {
        return price;
    }
}
