package edu.cs6510.servicebased.analytics.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/** 功能：映射已计算的窗口；windowEnd 唯一，作为分析重启恢复的检查点。 */

@Entity
@Table(name = "analytics_windows")
public class AnalyticsWindow {

    // 功能：下面的 JPA 字段/关联映射Analytics 私有数据库列；约束由本服务 database/schema.sql 创建。
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private int windowSize;

    @Column(nullable = false)
    private int slideInterval;

    @Column(nullable = false)
    private long windowStart;

    @Column(nullable = false, unique = true)
    private long windowEnd;

    @Column(nullable = false)
    private Instant computedAt;

    // 功能：构造器：供 JPA 反射创建实体。
    protected AnalyticsWindow() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public AnalyticsWindow(
            int windowSize,
            int slideInterval,
            long windowStart,
            long windowEnd,
            Instant computedAt
    ) {
        this.windowSize = windowSize;
        this.slideInterval = slideInterval;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.computedAt = computedAt;
    }

    // 功能：只读访问：返回数据库主键。
    public Long getId() {
        return id;
    }

    // 功能：只读访问：返回窗口配置大小。
    public int getWindowSize() {
        return windowSize;
    }

    // 功能：只读访问：返回窗口滑动步长。
    public int getSlideInterval() {
        return slideInterval;
    }

    // 功能：只读访问：返回窗口起点序号。
    public long getWindowStart() {
        return windowStart;
    }

    // 功能：只读访问：返回窗口终点序号。
    public long getWindowEnd() {
        return windowEnd;
    }

    // 功能：只读访问：返回结果计算时间。
    public Instant getComputedAt() {
        return computedAt;
    }
}
