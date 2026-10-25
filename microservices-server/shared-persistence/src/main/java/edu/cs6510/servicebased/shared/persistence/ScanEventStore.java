package edu.cs6510.servicebased.shared.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

/** 功能：Checkout 原子写原始 outbox；Analytics 批量生成连续日志，再按精确范围回放。 */

@Repository
public class ScanEventStore {
    private final JdbcTemplate jdbc;
    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public ScanEventStore(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional(propagation = Propagation.MANDATORY)
    // 功能：只写原始 outbox，加入调用方购物篮事务；IDENTITY 允许回滚空洞，不申请全局锁。
    public void append(String sku) {
        if (jdbc.update("INSERT INTO scan_outbox(sku) VALUES (?)", sku) != 1)
            throw new IllegalStateException("Scan event was not recorded");
    }
    //把购物篮修改和原始扫描事件一起提交到 scan_outbox，再由 Analytics 每批处理最多 1,000 条事件、生成连续分析编号。Checkout 不需要申请这个全局编号锁，减少了跨收银台的等待
    // 功能：将最多 1,000 条已提交原始事件转成连续日志，并在同一事务标记已处理。
    @Transactional
    public int promotePending() {
        // 功能：只串行化 Analytics 的批处理；Checkout 不使用该锁，扫描提交不需等待分析。
        // 锁与后面的 CTE 分成两条语句，使等待锁结束后获得新的 READ COMMITTED 快照。
        jdbc.execute("SELECT pg_advisory_xact_lock(6510, 3)");

        // 功能：先锁住本批已提交事件，再按原始 ID 排序并追加无空洞的 canonical 序号。
        // 并发事务的原始 ID 可有空洞或较晚提交；全局分析顺序由已提交批次及批内 ID 定义。
        // INSERT 的 RETURNING 驱动状态更新；任何一步失败都会将日志和 processed 一起回滚。
        return jdbc.update("""
                WITH pending AS MATERIALIZED (
                    SELECT event_id, sku
                    FROM scan_outbox
                    WHERE processed = false
                    ORDER BY event_id
                    LIMIT 1000
                    FOR UPDATE
                ), inserted AS (
                    INSERT INTO scan_events(sequence, sku, source_event_id)
                    SELECT base.last_sequence + row_number() OVER (ORDER BY p.event_id),
                           p.sku, p.event_id
                    FROM pending p
                    CROSS JOIN (
                        SELECT COALESCE(MAX(sequence), 0) AS last_sequence FROM scan_events
                    ) base
                    RETURNING source_event_id
                )
                UPDATE scan_outbox o
                SET processed = true
                FROM inserted i
                WHERE o.event_id = i.source_event_id
                """);
    }

    // 功能：返回已提交 canonical 日志的最大序号；该序号连续，因此也是记录数量。
    public long count() {
        return jdbc.queryForObject("SELECT COALESCE(MAX(sequence), 0) FROM scan_events", Long.class);
    }

    // 功能：按序号读取闭区间内的 SKU，供滑动窗口重建精确输入。
    public List<String> between(long first, long last) {
        return jdbc.queryForList("SELECT sku FROM scan_events WHERE sequence BETWEEN ? AND ? "
                + "ORDER BY sequence", String.class, first, last);
    }
}
