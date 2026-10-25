package edu.cs6510.servicebased.checkout.persistence;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

/** 功能：只访问 Checkout 私有 outbox；查询与确认各用短事务，不跨 HTTP 持有数据库锁。 */
@Repository
public class OutboxStore {
 public static final int BATCH_SIZE=500;
 public record Event(String eventId,String sku,String name) {}
 private final JdbcTemplate jdbc;
 public OutboxStore(JdbcTemplate jdbc) { this.jdbc=jdbc; }
 @Transactional(readOnly=true)
 public List<Event> pending() {
  return jdbc.query("SELECT event_key,sku,item_name FROM scan_outbox WHERE processed=false ORDER BY event_id LIMIT "+BATCH_SIZE,
   (rs,n)->new Event(rs.getString(1),rs.getString(2),rs.getString(3)));
 }
 @Transactional
 public void acknowledge(List<Event> events) {
  if(events.isEmpty()) return;
  // 一次往返确认整批；HTTP 响应丢失时不执行此更新，重放仍使用原事件 ID。
  jdbc.update(connection->{
   var statement=connection.prepareStatement("UPDATE scan_outbox SET processed=true WHERE event_key=ANY(?) AND processed=false");
   statement.setArray(1,connection.createArrayOf("varchar",events.stream().map(Event::eventId).toArray(String[]::new)));
   return statement;
  });
 }
}
