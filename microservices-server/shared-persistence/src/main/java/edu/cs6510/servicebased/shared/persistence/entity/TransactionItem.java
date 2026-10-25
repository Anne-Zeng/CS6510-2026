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
import jakarta.persistence.UniqueConstraint;

import java.math.BigDecimal;

/** 功能：映射购物篮明细；transaction_id + sku 唯一，重复扫描增加数量。 */

@Entity
@Table(

        name = "transaction_items",

        uniqueConstraints = @UniqueConstraint(
                name = "unique_transaction_sku",
                columnNames = {"transaction_id", "sku"}
        )
)

public class TransactionItem {

    // 功能：下面的 JPA 字段/关联映射集中式数据库列；约束由 database/schema.sql 统一创建。
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "transaction_id", nullable = false)
    private CheckoutTransaction transaction;

    @Column(nullable = false, length = 32)
    private String sku;

    @Column(name = "item_name", nullable = false)
    private String name;

    @Column(name = "unit_price", nullable = false, precision = 10, scale = 2)
    private BigDecimal unitPrice;

    @Column(nullable = false)
    private int quantity;

    // 功能：构造器：供 JPA 反射创建实体。
    protected TransactionItem() {

    }

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public TransactionItem(
            CheckoutTransaction transaction,
            String sku,
            String name,
            BigDecimal unitPrice
    ) {
        this.transaction = transaction;
        this.sku = sku;
        this.name = name;
        this.unitPrice = unitPrice;
        this.quantity = 1;
    }

    // 功能：同一 SKU 再次扫描时增加该明细的数量。
    public void addOne() {
        this.quantity++;
    }

    // 功能：只读访问：返回数据库主键。
    public Long getId() {
        return id;
    }

    // 功能：只读访问：返回所属交易。
    public CheckoutTransaction getTransaction() {
        return transaction;
    }

    // 功能：只读访问：返回商品 SKU。
    public String getSku() {
        return sku;
    }

    // 功能：只读访问：返回商品名称。
    public String getName() {
        return name;
    }

    // 功能：只读访问：返回扫描时保存的单价。
    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    // 功能：只读访问：返回商品数量。
    public int getQuantity() {
        return quantity;
    }
}
