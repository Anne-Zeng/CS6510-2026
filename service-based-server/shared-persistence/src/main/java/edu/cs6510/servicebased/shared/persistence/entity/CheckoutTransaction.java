package edu.cs6510.servicebased.shared.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;

/** 功能：映射交易主表；Version 检测并发修改，状态和金额随事务提交。 */

@Entity
@Table(name = "checkout_transactions")

public class CheckoutTransaction {

    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Version
    private long version;

    @Id
    @Column(name = "transaction_id", nullable = false, updatable = false, length = 36)

    private String transactionId;

    @Column(nullable = false)
    private String stationId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    @Column(nullable = false)
    private int itemCount;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal runningTotal;

    @Column(nullable = false)
    private Instant startedAt;

    private Instant completedAt;

    // 功能：构造器：供 JPA 反射创建实体。
    protected CheckoutTransaction() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public CheckoutTransaction(String transactionId, String stationId, Instant startedAt) {
        this.transactionId = transactionId;
        this.stationId = stationId;
        this.status = TransactionStatus.OPEN;
        this.itemCount = 0;
        this.runningTotal = new BigDecimal("0.00");
        this.startedAt = startedAt;
    }

    // 功能：每次成功扫描增加购物篮总件数，并累加该商品单价。
    public void addScannedItem(BigDecimal unitPrice) {
        this.itemCount++;
        this.runningTotal = this.runningTotal.add(unitPrice);
    }

    // 功能：将交易标记为完成，并记录完成时间；最终由数据库事务提交。
    public void complete(Instant completedAt) {
        this.status = TransactionStatus.COMPLETED;
        this.completedAt = completedAt;
    }

    // 功能：只读访问：返回交易标识。
    public String getTransactionId() {
        return transactionId;
    }

    // 功能：只读访问：返回收银台标识。
    public String getStationId() {
        return stationId;
    }

    // 功能：只读访问：返回交易状态。
    public TransactionStatus getStatus() {
        return status;
    }

    // 功能：只读访问：返回购物篮总件数。
    public int getItemCount() {
        return itemCount;
    }

    // 功能：只读访问：返回购物篮累计金额。
    public BigDecimal getRunningTotal() {
        return runningTotal;
    }

    // 功能：只读访问：返回交易创建时间。
    public Instant getStartedAt() {
        return startedAt;
    }

    // 功能：只读访问：返回交易完成时间。
    public Instant getCompletedAt() {
        return completedAt;
    }
}
