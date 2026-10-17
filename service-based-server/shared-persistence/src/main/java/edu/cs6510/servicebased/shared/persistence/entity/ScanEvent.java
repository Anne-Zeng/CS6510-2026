package edu.cs6510.servicebased.shared.persistence.entity;

import jakarta.persistence.*;

/** 功能：映射持久化扫描日志；Checkout 写入，Analytics 只读取已提交记录。 */

@Entity
@Table(name = "scan_events")
public class ScanEvent {
    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Id
    private long sequence;
    @Column(nullable = false, length = 32)
    private String sku;
    // 功能：构造器：供 JPA 反射创建实体。
    protected ScanEvent() {}
}
