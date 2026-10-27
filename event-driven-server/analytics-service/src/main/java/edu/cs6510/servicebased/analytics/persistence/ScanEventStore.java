package edu.cs6510.servicebased.analytics.persistence;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

/** 功能：只将本服务 inbox 整理成连续分析序号；接收次序允许跨批次不同于扫描提交次序。 */
@Repository
public class ScanEventStore {
 private final JdbcTemplate jdbc;
 public ScanEventStore(JdbcTemplate jdbc) { this.jdbc=jdbc; }
 @Transactional
 public int promotePending() {
  jdbc.execute("SELECT pg_advisory_xact_lock(6510, 3)");
  return jdbc.update("""
   WITH pending AS MATERIALIZED (
    SELECT received_id,event_key,sku,name FROM scan_inbox WHERE processed=false
    ORDER BY received_id LIMIT 1000 FOR UPDATE
   ), inserted AS (
    INSERT INTO scan_events(sequence,event_key,sku,name)
    SELECT base.last_sequence+row_number() OVER(ORDER BY p.received_id),p.event_key,p.sku,p.name
    FROM pending p CROSS JOIN (SELECT COALESCE(MAX(sequence),0) AS last_sequence FROM scan_events) base
    RETURNING event_key
   )
   UPDATE scan_inbox inbox SET processed=true FROM inserted i WHERE inbox.event_key=i.event_key
   """);
 }
 public long count() { return jdbc.queryForObject("SELECT COALESCE(MAX(sequence),0) FROM scan_events",Long.class); }
 public List<String> between(long first,long last) {
  return jdbc.queryForList("SELECT sku FROM scan_events WHERE sequence BETWEEN ? AND ? ORDER BY sequence",String.class,first,last);
 }
}
