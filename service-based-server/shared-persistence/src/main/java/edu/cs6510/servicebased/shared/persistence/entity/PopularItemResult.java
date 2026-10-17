package edu.cs6510.servicebased.shared.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/** 功能：映射某个窗口的排名行；与窗口元数据在同一事务内保存。 */

@Entity
@Table(name = "popular_item_results")
public class PopularItemResult {

    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "window_id", nullable = false)
    private AnalyticsWindow window;

    @Column(nullable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int scanCount;

    @Column(nullable = false)
    private int rank;

    // 功能：构造器：供 JPA 反射创建实体。
    protected PopularItemResult() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public PopularItemResult(
            AnalyticsWindow window,
            String sku,
            String name,
            int scanCount,
            int rank
    ) {
        this.window = window;
        this.sku = sku;
        this.name = name;
        this.scanCount = scanCount;
        this.rank = rank;
    }

    // 功能：只读访问：返回商品 SKU。
    public String getSku() {
        return sku;
    }

    // 功能：只读访问：返回商品名称。
    public String getName() {
        return name;
    }

    // 功能：只读访问：返回该窗口内的扫描次数。
    public int getScanCount() {
        return scanCount;
    }

    // 功能：只读访问：返回排名名次。
    public int getRank() {
        return rank;
    }
}
