package edu.cs6510.servicebased.inventory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.time.Instant;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import static org.springframework.http.HttpStatus.*;

/** 功能：整篮扣减由 DebitWriter 合并为自动提交 SQL，热点库存行锁只在数据库语句内持有；其他写操作的许可等待发生在获取数据库连接之前。 */
@Service
public class InventoryService {
 public record Line(@NotBlank @Size(max=32) String sku, @Positive int quantity) {}
 public record Request(@NotEmpty @Size(max=1000) List<@NotNull @Valid Line> items) {}
 public record Reservation(String transactionId, String status) {}
 public record Alert(String sku, String name, int currentStock, int threshold, Instant triggeredAt) {}
 public record LowStock(int threshold, Instant generatedAt, List<Alert> alerts) {}
 private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(InventoryService.class);
 @Value("${inventory.performance-log:false}") private boolean performanceLog;
 private final java.util.concurrent.atomic.LongAdder calls=new java.util.concurrent.atomic.LongAdder(),waitNanos=new java.util.concurrent.atomic.LongAdder(),transactionNanos=new java.util.concurrent.atomic.LongAdder();
 private final JdbcTemplate jdbc;
 private final TransactionTemplate transactions;
 private final DebitWriter debits;
 private final Semaphore inFlight;
 private final int threshold;
 public InventoryService(JdbcTemplate jdbc, TransactionTemplate transactions, DebitWriter debits,
     @Value("${inventory.low-stock-threshold:50}") int threshold,
     @Value("${inventory.max-in-flight:8}") int maximum) {
  if(threshold<0 || maximum<1) throw new IllegalArgumentException("Invalid inventory configuration");
  this.jdbc=jdbc;this.transactions=transactions;this.debits=debits;this.threshold=threshold;this.inFlight=new Semaphore(maximum,true);
 }
 private void checkId(String id) {
  if(id==null || id.isBlank() || id.length()>100) throw new ResponseStatusException(BAD_REQUEST,"Invalid transactionId");
 }
 private <T> T write(Supplier<T> operation) {
  boolean acquired=false;
  long started=System.nanoTime(),acquiredAt=started;
  try {
   acquired=inFlight.tryAcquire(2,TimeUnit.SECONDS);
   if(!acquired) throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Inventory busy; retry the same operation");
   acquiredAt=System.nanoTime();
   return operation.get();
  } catch(InterruptedException ex) {
   Thread.currentThread().interrupt();throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Inventory interrupted",ex);
  } finally {
   if(acquired) inFlight.release();
   if(performanceLog) {
    waitNanos.add(acquiredAt-started);transactionNanos.add(System.nanoTime()-acquiredAt);calls.increment();
    long count=calls.sum();
    if(count%1000==0) log.info("Inventory timing: calls={} gateMeanMs={} transactionMeanMs={}",count,waitNanos.sum()/1e6/count,transactionNanos.sum()/1e6/count);
   }
  }
 }
 /** 功能：Saga 的关键一步：按 SKU 合并后一次扣减整篮并直接确认，成功后 Checkout 只剩可重试的本地完成。 */
 public Reservation reserve(String id,Request request) {
  checkId(id);
  if(request==null || request.items()==null || request.items().isEmpty() || request.items().size()>1000)
   throw new ResponseStatusException(BAD_REQUEST,"Invalid basket");
  var lines=new TreeMap<String,Integer>();
  try {
   for(var line:request.items()) {
    if(line==null || line.sku()==null || line.sku().isBlank() || line.sku().length()>32 || line.quantity()<=0)
     throw new ResponseStatusException(BAD_REQUEST,"Invalid item");
    lines.merge(line.sku(),line.quantity(),Math::addExact);
   }
  } catch(ArithmeticException ex) { throw new ResponseStatusException(BAD_REQUEST,"Quantity overflow"); }
  // 功能：新 ID 交给单一写入线程与同时到达的购物篮合并扣减；等待期间不占许可和数据库连接。
  if(debits.debit(id,lines)) return new Reservation(id,"CONFIRMED");
  // 功能：相同 ID 已存在时只做幂等核对，不再扣库存；行锁与并发释放串行化。
  return write(()->transactions.execute(status->{
   String state=lock(id);
   if(state.equals("CANCELLED")) throw new ResponseStatusException(CONFLICT,"Reservation cancelled");
   if(!Set.of("RESERVED","CONFIRMED").contains(state)) throw new IllegalStateException("Unexpected reservation state");
   var saved=new TreeMap<String,Integer>();
   jdbc.query("SELECT sku,quantity FROM reservation_items WHERE transaction_id=?",rs->{ saved.put(rs.getString(1),rs.getInt(2)); },id);
   if(!saved.equals(lines)) throw new ResponseStatusException(CONFLICT,"Idempotency payload mismatch");
   return new Reservation(id,state);
  }));
 }
 private String lock(String id) {
  return jdbc.queryForObject("SELECT status FROM reservations WHERE transaction_id=? FOR UPDATE",String.class,id);
 }
 @Transactional(readOnly=true)
 public Reservation get(String id) {
  checkId(id);
  var rows=jdbc.queryForList("SELECT status FROM reservations WHERE transaction_id=?",String.class,id);
  if(rows.isEmpty()) throw new ResponseStatusException(NOT_FOUND,"Reservation not found");
  return new Reservation(id,rows.getFirst());
 }
 /** 功能：兼容上一版两阶段预留遗留的 RESERVED 记录；一条自动提交的条件 UPDATE 通过行锁与释放请求串行化。 */
 public Reservation confirm(String id) {
  checkId(id);
  return write(()->{
   var rows=jdbc.queryForList("UPDATE reservations SET status='CONFIRMED' WHERE transaction_id=? AND status IN ('RESERVED','CONFIRMED') RETURNING status",String.class,id);
   if(rows.isEmpty()) {
    get(id);throw new ResponseStatusException(CONFLICT,"Cannot confirm reservation");
   }
   return new Reservation(id,"CONFIRMED");
  });
 }
 /** 功能：Saga 补偿：归还该 ID 已扣的整篮库存并留下 CANCELLED 墓碑，迟到的扣减请求不会再扣库存；使用相同锁顺序。
  * Checkout 只在库存明确拒绝并持久化取消决定后调用；已扣库存只可能来自迟到的重复请求或上一版的 RESERVED。 */
 public Reservation release(String id) {
  checkId(id);
  return write(()->transactions.execute(status->{
   var inserted=jdbc.queryForList("INSERT INTO reservations(transaction_id,status) VALUES (?,'CANCELLED') ON CONFLICT DO NOTHING RETURNING transaction_id",String.class,id);
   if(!inserted.isEmpty()) return new Reservation(id,"CANCELLED");
   String state=lock(id);
   if(!state.equals("CANCELLED")) jdbc.update("""
    WITH locked AS MATERIALIZED (
     SELECT stock.sku,line.quantity FROM inventory stock JOIN reservation_items line USING(sku)
     WHERE line.transaction_id=? ORDER BY stock.sku FOR UPDATE OF stock
    ) UPDATE inventory stock SET current_stock=stock.current_stock+locked.quantity FROM locked WHERE stock.sku=locked.sku
    """,id);
   jdbc.update("UPDATE reservations SET status='CANCELLED' WHERE transaction_id=?",id);
   return new Reservation(id,"CANCELLED");
  }));
 }
 @Transactional(readOnly=true)
 public LowStock lowStock(Integer requested) {
  int limit=requested==null?threshold:requested;
  if(limit<0) throw new ResponseStatusException(BAD_REQUEST,"Negative threshold");
  Instant now=Instant.now();
  var rows=jdbc.query("SELECT sku,name,current_stock FROM inventory WHERE current_stock<? ORDER BY sku",(rs,n)->new Alert(rs.getString(1),rs.getString(2),rs.getInt(3),limit,now),limit);
  return new LowStock(limit,now,rows);
 }
}
