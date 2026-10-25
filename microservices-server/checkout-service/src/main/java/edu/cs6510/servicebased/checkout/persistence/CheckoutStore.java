package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.ScanResult;
import edu.cs6510.servicebased.checkout.contract.TransactionResponse;
import edu.cs6510.servicebased.checkout.contract.CatalogItemResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.CONFLICT;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.NOT_FOUND;

/** 功能：封装 Checkout 私有数据库 SQL；减少网络往返，并保留数据库事务和行锁保护。 */
@Repository
@Transactional(propagation = Propagation.MANDATORY)
public class CheckoutStore {
    // 多语句读取共享调用者的事务；create/scan 显式使用 PostgreSQL 单语句原子事务。
    private final JdbcTemplate jdbc;

    // 功能：注入数据库访问对象；只访问 Checkout 私有数据库。
    public CheckoutStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 功能：直接插入 OPEN 购物篮，避免 JPA 为带 UUID 的新实体先执行存在性查询。
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void create(String id, String stationId, Instant startedAt) {
        jdbc.update("""
                INSERT INTO checkout_transactions
                    (transaction_id, version, station_id, status, item_count, running_total, started_at)
                VALUES (?, 0, ?, 'OPEN', 0, 0.00, ?)
                """, id, stationId, Timestamp.from(startedAt));
    }

    // 功能：只读取接口所需字段，不构造实体图；找不到交易时维持既有 404 业务错误。
    public TransactionResponse find(String id) {
        return required(jdbc.query("SELECT * FROM checkout_transactions WHERE transaction_id = ?",
                CheckoutStore::transaction, id));
    }

    // 功能：一条 SQL 锁交易、合并明细、累计汇总并写持久化 outbox，不等待全局扫描编号。这个方法通过一次 jdbc.query(...)，将一整段 WITH ... SELECT ... SQL 交给 PostgreSQL。
    // 单条 PostgreSQL 语句本身就是原子事务；省去额外 BEGIN/COMMIT 往返，篮子和 outbox 仍一起落盘。
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public ScanResult scan(String id, CatalogItemResponse item) {
        var results = jdbc.query("""
                WITH locked AS MATERIALIZED (
                    SELECT transaction_id, status FROM checkout_transactions
                    WHERE transaction_id = ? FOR UPDATE
                ), line AS (
                    INSERT INTO transaction_items
                        (transaction_id, sku, item_name, unit_price, quantity)
                    SELECT transaction_id, ?, ?, ?, 1 FROM locked WHERE status = 'OPEN'
                    ON CONFLICT (transaction_id, sku)
                    DO UPDATE SET quantity = transaction_items.quantity + 1
                    RETURNING sku, item_name, unit_price
                ), changed AS (
                    UPDATE checkout_transactions AS tx
                    SET item_count = tx.item_count + 1,
                        running_total = tx.running_total + line.unit_price,
                        version = tx.version + 1
                    FROM line WHERE tx.transaction_id = ?
                    RETURNING tx.item_count, tx.running_total
                ), recorded AS (
                    INSERT INTO scan_outbox(sku,item_name)
                    SELECT line.sku,line.item_name FROM line CROSS JOIN changed
                    RETURNING event_id
                )
                SELECT locked.status, line.sku, line.item_name, line.unit_price,
                       changed.item_count, changed.running_total, recorded.event_id
                FROM locked LEFT JOIN line ON true LEFT JOIN changed ON true LEFT JOIN recorded ON true
                """, (row, index) -> {
            // 功能：已完成的购物篮不会进入写入 CTE；这里把无写入的状态转换为业务错误。
            requireOpen(row.getString("status"));
            // 功能：检查 SQL 返回的 outbox 标识；数据库约束错误会让整个语句回滚。
            if (row.getLong("event_id") <= 0) throw new IllegalStateException("Scan event was not recorded");
            return new ScanResult(id, row.getString("sku"), row.getString("item_name"),
                    row.getBigDecimal("unit_price"), row.getInt("item_count"),
                    row.getBigDecimal("running_total"));
        }, id, item.sku(), item.name(), item.price(), id);
        return required(results);
    }

    // 功能：统一转换数据库交易列，保持现有 HTTP 字段及十进制金额精度。
    private static TransactionResponse transaction(ResultSet row, int index) throws SQLException {
        return new TransactionResponse(row.getString("transaction_id"), row.getString("station_id"),
                row.getString("status"), row.getInt("item_count"), row.getBigDecimal("running_total"),
                row.getTimestamp("started_at").toInstant());
    }

    // 功能：为查询与写入路径保持相同的“交易不存在”错误格式。
    private static <T> T required(List<T> rows) {
        if (rows.isEmpty()) {
            throw new BusinessException(NOT_FOUND, "TRANSACTION_NOT_FOUND", "Transaction not found");
        }
        return rows.getFirst();
    }

    // 功能：只允许 OPEN 状态接受扫描或完成，阻止重复完成和完成后新增商品。
    private static void requireOpen(String status) {
        if (!"OPEN".equals(status)) {
            throw new BusinessException(CONFLICT, "TRANSACTION_NOT_OPEN", "Transaction is not open");
        }
    }
}
