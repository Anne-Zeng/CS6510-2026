package edu.cs6510.servicebased.analytics;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import static org.springframework.http.HttpStatus.CONFLICT;
import java.util.List;
import java.util.ArrayList;
import java.util.StringJoiner;

/** 功能：HTTP 接收批次在一个本地事务中去重并持久化，提交后才返回确认。 */
@Service
public class EventInbox {
 public record Event(@NotBlank @Pattern(regexp="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}") String eventId,
                     @NotBlank @Size(max=32) String sku, @NotBlank @Size(max=255) String name) {}
 public record Batch(@NotEmpty @Size(max=1000) List<@NotNull @Valid Event> events) {}
 public record Ack(List<String> acceptedEventIds) {}
 private final JdbcTemplate jdbc;
 public EventInbox(JdbcTemplate jdbc) { this.jdbc=jdbc; }
 @Transactional
 public Ack accept(Batch batch) {
  // 功能：与旧 inbox 恢复共用编号锁；新事件在接收事务内一次写入连续日志。
  jdbc.execute("SELECT pg_advisory_xact_lock(6510, 3)");
  // 功能：一条批量 SQL 插入并验证整批。RETURNING 读取本语句新行，表查询读取已有行。
  var values=new StringJoiner(",");var parameters=new ArrayList<Object>();
  int ordinal=0;
  for(var event:batch.events()) {
   values.add("(?,?,?,"+(ordinal++)+")");
   parameters.add(event.eventId());parameters.add(event.sku());parameters.add(event.name());
  }
  long matches=jdbc.queryForObject("""
   WITH incoming(event_key,sku,name,ordinal) AS (VALUES %s), unique_events AS MATERIALIZED (
    SELECT DISTINCT ON(event_key) event_key,sku,name,ordinal FROM incoming ORDER BY event_key,ordinal
   ), inserted AS (
    INSERT INTO scan_inbox(event_key,sku,name,processed)
    SELECT event_key,sku,name,true FROM unique_events ORDER BY ordinal
    ON CONFLICT(event_key) DO NOTHING RETURNING event_key,sku,name
   ), canonical AS (
    INSERT INTO scan_events(sequence,event_key,sku,name)
    SELECT base.last_sequence+row_number() OVER(ORDER BY u.ordinal),n.event_key,n.sku,n.name
    FROM inserted n JOIN unique_events u USING(event_key)
    CROSS JOIN (SELECT COALESCE(MAX(sequence),0) AS last_sequence FROM scan_events) base
    RETURNING event_key
   ) SELECT count(*) FROM incoming i
   WHERE EXISTS(SELECT 1 FROM inserted n WHERE n.event_key=i.event_key AND n.sku=i.sku AND n.name=i.name)
      OR EXISTS(SELECT 1 FROM scan_inbox s WHERE s.event_key=i.event_key AND s.sku=i.sku AND s.name=i.name)
   """.formatted(values),Long.class,parameters.toArray());
  if(matches!=batch.events().size()) throw new ResponseStatusException(CONFLICT,"Event ID reused with different payload");
  return new Ack(batch.events().stream().map(Event::eventId).toList());
 }
}
