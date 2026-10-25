package edu.cs6510.servicebased.shared.persistence;

import edu.cs6510.servicebased.shared.common.BusinessException;
import edu.cs6510.servicebased.shared.contract.ReceiptLine;
import edu.cs6510.servicebased.shared.contract.ScanResult;
import edu.cs6510.servicebased.shared.contract.TransactionResponse;
import edu.cs6510.servicebased.shared.persistence.entity.CatalogItem;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static edu.cs6510.servicebased.shared.common.BusinessException.Kind.CONFLICT;
import static edu.cs6510.servicebased.shared.common.BusinessException.Kind.NOT_FOUND;

/** 功能：集中封装结账热路径 SQL；减少网络往返，并保留数据库事务和行锁保护。 */
@Repository
@Transactional(propagation = Propagation.MANDATORY)
public class CheckoutStore {
    // 功能：共享 Spring 当前事务绑定的连接，购物篮、日志和库存操作按业务边界提交。
    private final JdbcTemplate jdbc;

    // 功能：注入数据库访问对象；本共享模块不启动服务，也不发起服务间调用。
    public CheckoutStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 功能：直接插入 OPEN 购物篮，避免 JPA 为带 UUID 的新实体先执行存在性查询。
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
    public ScanResult scan(String id, CatalogItem item) {
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
                    INSERT INTO scan_outbox(sku)
                    SELECT line.sku FROM line CROSS JOIN changed
                    RETURNING event_id
                )
                SELECT locked.status, line.sku, line.item_name, line.unit_price,
                       changed.item_count, changed.running_total, recorded.event_id
                FROM locked LEFT JOIN line ON true LEFT JOIN changed ON true LEFT JOIN recorded ON true
                """, (row, index) -> {
            // 功能：已完成的购物篮不会进入写入 CTE；这里把无写入的状态转换为业务错误。
            requireOpen(row.getString("status"));
            // 功能：未确认 outbox 插入就抛异常；外层 Spring 事务回滚购物篮与事件的全部写入。
            if (row.getLong("event_id") <= 0) throw new IllegalStateException("Scan event was not recorded");
            return new ScanResult(id, row.getString("sku"), row.getString("item_name"),
                    row.getBigDecimal("unit_price"), row.getInt("item_count"),
                    row.getBigDecimal("running_total"));
        }, id, item.getSku(), item.getName(), item.getPrice(), id);
        return required(results);
    }

    // 功能：完成交易前先锁主行；并发扫描或第二个完成请求必须等前一个事务提交后重新检查状态。
    public TransactionResponse lockOpen(String id) {
        TransactionResponse tx = required(jdbc.query(
                "SELECT * FROM checkout_transactions WHERE transaction_id = ? FOR UPDATE",
                CheckoutStore::transaction, id));
        requireOpen(tx.status());
        return tx;
    }

    // 功能：读取已锁交易的稳定购物篮；SKU 顺序同时用于收据展示和库存锁顺序。
    public List<ReceiptLine> lines(String id) {
        return jdbc.query("""
                SELECT sku, item_name, unit_price, quantity FROM transaction_items
                WHERE transaction_id = ? ORDER BY sku
                """, (row, index) -> new ReceiptLine(row.getString("sku"), row.getString("item_name"),
                row.getBigDecimal("unit_price"), row.getInt("quantity")), id);
    }

    // 功能：返回两类写入的确认结果，调用方必须同时核对库存行数和交易状态更新行数。
    public record CompletionWrite(int debitedLines, int completedTransactions) { }

    // 功能：一次往返锁库存、条件扣减并完成交易；不足时由外层事务回滚全部库存变化。
    public CompletionWrite completeWithInventory(String id, int expectedLines, int threshold, Instant now) {
        if (expectedLines <= 0) throw new IllegalArgumentException("Completion requires basket lines");
        return jdbc.queryForObject("""
                WITH locked_stock AS MATERIALIZED (
                    SELECT stock.sku, line.quantity
                    FROM inventory AS stock
                    JOIN transaction_items AS line ON line.sku = stock.sku
                    WHERE line.transaction_id = ?
                    ORDER BY stock.sku FOR UPDATE OF stock
                ), debited AS (
                    UPDATE inventory AS stock
                    SET current_stock = stock.current_stock - locked_stock.quantity,
                        low_stock_triggered_at = CASE
                            WHEN stock.current_stock - locked_stock.quantity < ?
                            THEN COALESCE(stock.low_stock_triggered_at, ?)
                            ELSE stock.low_stock_triggered_at END
                    FROM locked_stock
                    WHERE stock.sku = locked_stock.sku
                      AND stock.current_stock >= locked_stock.quantity
                    RETURNING stock.sku
                ), debit_count AS MATERIALIZED (
                    SELECT count(*) AS total FROM debited
                ), completed AS (
                    UPDATE checkout_transactions
                    SET status = 'COMPLETED', completed_at = ?, version = version + 1
                    WHERE transaction_id = ? AND status = 'OPEN'
                      AND (SELECT total FROM debit_count) = ?
                    RETURNING transaction_id
                )
                SELECT debit_count.total AS debited_lines,
                       (SELECT count(*) FROM completed) AS completed_transactions
                FROM debit_count
                """, (row, index) -> new CompletionWrite(row.getInt("debited_lines"),
                row.getInt("completed_transactions")), id, threshold, Timestamp.from(now),
                Timestamp.from(now), id, expectedLines);
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
