package edu.cs6510.servicebased.shared.persistence.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** 功能：映射库存表；记录剩余数量和首次跌破阈值的时间。 */

@Entity
@Table(name = "inventory")
public class Inventory {

    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Id
    @Column(nullable = false, updatable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private int currentStock;

    @Column(name = "low_stock_triggered_at")
    private Instant lowStockTriggeredAt;

    // 功能：构造器：供 JPA 反射创建实体。
    protected Inventory() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public Inventory(String sku, int currentStock) {
        this.sku = sku;
        this.currentStock = currentStock;
    }

    // 功能：仅首次记录低库存触发时间，保留最早的警报时间。
    public void markLowStock(Instant triggeredAt) {
        if (this.lowStockTriggeredAt == null) {
            this.lowStockTriggeredAt = triggeredAt;
        }
    }

    // 功能：只读访问：返回商品 SKU。
    public String getSku() {
        return sku;
    }

    // 功能：只读访问：返回当前剩余库存。
    public int getCurrentStock() {
        return currentStock;
    }

    // 功能：只读访问：返回首次低库存时间。
    public Instant getLowStockTriggeredAt() {
        return lowStockTriggeredAt;
    }
}
