package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.*;

/** 功能：冻结时一次读购物篮；每个 Saga 步骤是一条自动提交 SQL，租约与业务状态一起提交，网络等待不占数据库事务。 */
@Repository
public class SagaStore {
 public record Work(String id,String transactionId,String state,String owner,
                    TransactionResponse transaction,List<ReceiptLine> lines) {
  public Work { lines=List.copyOf(lines); }
  public Work withState(String next) { return new Work(id,transactionId,next,owner,transaction,lines); }
 }
 public record Start(Work work,Receipt receipt) {}
 private final JdbcTemplate jdbc;
 public SagaStore(JdbcTemplate jdbc) { this.jdbc=jdbc; }
 private static TransactionResponse transaction(ResultSet rs) throws SQLException {
  return new TransactionResponse(rs.getString("transaction_id"),rs.getString("station_id"),rs.getString("status"),
    rs.getInt("item_count"),rs.getBigDecimal("running_total"),rs.getTimestamp("started_at").toInstant());
 }
 private static ReceiptLine line(ResultSet rs) throws SQLException {
  return new ReceiptLine(rs.getString("sku"),rs.getString("item_name"),rs.getBigDecimal("unit_price"),rs.getInt("quantity"));
 }
 private static Receipt receipt(TransactionResponse transaction,java.time.Instant completed,List<ReceiptLine> lines) {
  return new Receipt(transaction.transactionId(),transaction.stationId(),transaction.itemCount(),transaction.runningTotal(),transaction.startedAt(),completed,lines);
 }
 /** 功能：一条自动提交 SQL 锁交易、创建租约 Saga、冻结购物篮并返回明细；重复完成直接返回持久收据。
  * READ COMMITTED 下行锁返回最新交易行，而明细来自语句快照；只有件数与快照明细一致才冻结。
  * 不一致说明并发扫描在快照之后提交，此时不写入并返回空 Start，由协调者稍后重新开始。 */
 public Start begin(String id) {
  String saga=UUID.randomUUID().toString(),owner=UUID.randomUUID().toString();
  var rows=jdbc.query("""
    WITH tx AS MATERIALIZED (
     SELECT * FROM checkout_transactions WHERE transaction_id=? FOR UPDATE
    ), created AS (
     INSERT INTO checkout_sagas(saga_id,transaction_id,state,lease_owner,lease_until)
     SELECT ?,transaction_id,'RESERVING',?,now()+interval '30 seconds' FROM tx
     WHERE status='OPEN' AND item_count>0
      AND item_count=(SELECT COALESCE(sum(quantity),0) FROM transaction_items WHERE transaction_id=tx.transaction_id)
     RETURNING transaction_id
    ), frozen AS (
     UPDATE checkout_transactions t SET status='CHECKOUT_PENDING',version=t.version+1
     FROM created WHERE t.transaction_id=created.transaction_id RETURNING t.transaction_id
    ) SELECT tx.*,(SELECT count(*) FROM frozen) AS frozen,l.sku,l.item_name,l.unit_price,l.quantity
    FROM tx LEFT JOIN transaction_items l USING(transaction_id) ORDER BY l.sku
    """,(rs,n)->new Object[]{transaction(rs),rs.getTimestamp("completed_at"),rs.getInt("frozen"),
      rs.getString("sku")==null?null:line(rs)},id,saga,owner);
  if(rows.isEmpty()) throw new BusinessException(NOT_FOUND,"TRANSACTION_NOT_FOUND","Transaction not found");
  var transaction=(TransactionResponse)rows.getFirst()[0];
  var lines=rows.stream().map(row->(ReceiptLine)row[3]).filter(Objects::nonNull).toList();
  return switch(transaction.status()) {
   case "COMPLETED" -> new Start(null,receipt(transaction,((java.sql.Timestamp)rows.getFirst()[1]).toInstant(),lines));
   case "OPEN" -> {
    if(transaction.itemCount()==0) throw new BusinessException(CONFLICT,"EMPTY_TRANSACTION","Transaction basket is empty");
    yield (int)rows.getFirst()[2]==1?new Start(new Work(saga,id,"RESERVING",owner,transaction,lines),null):new Start(null,null);
   }
   case "CHECKOUT_PENDING" -> claimActive(transaction,owner);
   default -> throw new BusinessException(CONFLICT,"TRANSACTION_NOT_OPEN","Transaction is not open");
  };
 }
 /** 功能：已冻结交易只在租约过期且到达重试时间时由当前请求接管，并读取冻结购物篮。 */
 private Start claimActive(TransactionResponse transaction,String owner) {
  String id=transaction.transactionId();
  var claimed=jdbc.query("""
    WITH claimed AS (
     UPDATE checkout_sagas SET lease_owner=?,lease_until=now()+interval '30 seconds'
     WHERE transaction_id=? AND state IN ('RESERVING','COMMITTING','CANCELLING')
      AND next_attempt_at<=now() AND (lease_until IS NULL OR lease_until<now())
     RETURNING saga_id,state
    ) SELECT c.saga_id,c.state,l.* FROM claimed c JOIN transaction_items l ON l.transaction_id=? ORDER BY l.sku
    """,(rs,n)->new Object[]{rs.getString("saga_id"),rs.getString("state"),line(rs)},owner,id,id);
  if(claimed.isEmpty()) return new Start(null,null);
  return new Start(new Work((String)claimed.getFirst()[0],id,(String)claimed.getFirst()[1],owner,transaction,
    claimed.stream().map(row->(ReceiptLine)row[2]).toList()),null);
 }
 /** 功能：恢复时一条 SQL 领取租约并返回冻结上下文，不在每个阶段重新读取。 */
 public Work claim(String id) {
  String owner=UUID.randomUUID().toString();
  var rows=jdbc.query("""
    WITH claimed AS (
     UPDATE checkout_sagas SET lease_owner=?,lease_until=now()+interval '30 seconds'
     WHERE saga_id=? AND state IN ('RESERVING','COMMITTING','CANCELLING') AND next_attempt_at<=now()
      AND (lease_until IS NULL OR lease_until<now()) RETURNING saga_id,transaction_id,state
    ) SELECT c.saga_id,c.state AS saga_state,t.*,l.sku,l.item_name,l.unit_price,l.quantity
      FROM claimed c JOIN checkout_transactions t USING(transaction_id)
      JOIN transaction_items l USING(transaction_id) ORDER BY l.sku
    """,(rs,n)->new Object[]{rs.getString("saga_id"),rs.getString("saga_state"),transaction(rs),line(rs)},owner,id);
  if(rows.isEmpty()) return null;
  var transaction=(TransactionResponse)rows.getFirst()[2];
  return new Work(id,transaction.transactionId(),(String)rows.getFirst()[1],owner,transaction,
    rows.stream().map(row->(ReceiptLine)row[3]).toList());
 }
 /** 功能：租约令牌充当 fencing token，旧工作者不能覆盖新工作者的决定。 */
 public Work decide(Work work,String next) {
  if(!Set.of("COMMITTING","CANCELLING").contains(next)) throw new IllegalArgumentException("Invalid decision");
  int changed=jdbc.update("UPDATE checkout_sagas SET state=?,updated_at=now(),last_error=NULL WHERE saga_id=? AND lease_owner=? AND state='RESERVING'",next,work.id(),work.owner());
  if(changed!=1) throw new IllegalStateException("Checkout lease lost");
  return work.withState(next);
 }
 /** 功能：一条自动提交 SQL 原子完成交易、Saga 及租约清理；复用上下文生成收据。
  * RESERVING 表示库存已一步扣减确认；COMMITTING 来自上一版两阶段预留；CANCELLING 恢复购物篮。 */
 public Receipt finish(Work work) {
  boolean commit=!work.state().equals("CANCELLING");
  if(commit && !Set.of("RESERVING","COMMITTING").contains(work.state())) throw new IllegalArgumentException("Invalid final state");
  var rows=jdbc.query("""
    WITH locked AS MATERIALIZED (
     SELECT transaction_id FROM checkout_transactions WHERE transaction_id=? AND status='CHECKOUT_PENDING' FOR UPDATE
    ), owned AS MATERIALIZED (
     SELECT s.saga_id FROM checkout_sagas s CROSS JOIN locked
     WHERE s.saga_id=? AND s.lease_owner=? AND s.state=? FOR UPDATE OF s
    ), changed AS (
     UPDATE checkout_transactions SET status=?,completed_at=CASE WHEN ? THEN statement_timestamp() ELSE NULL END,version=version+1
     WHERE transaction_id=? AND EXISTS(SELECT 1 FROM owned) RETURNING completed_at
    ), finalized AS (
     UPDATE checkout_sagas SET state=?,lease_owner=NULL,lease_until=NULL,last_error=NULL,updated_at=now()
     WHERE saga_id=? AND lease_owner=? AND EXISTS(SELECT 1 FROM changed) RETURNING saga_id
    ) SELECT changed.completed_at,(SELECT count(*) FROM finalized) AS finalized_count FROM changed
    """,(rs,n)->new Object[]{rs.getTimestamp(1),rs.getInt(2)},work.transactionId(),work.id(),work.owner(),work.state(),
     commit?"COMPLETED":"OPEN",commit,work.transactionId(),commit?"DONE":"ABORTED",work.id(),work.owner());
  if(rows.size()!=1 || (int)rows.getFirst()[1]!=1) throw new IllegalStateException("Checkout lease lost or pending transaction missing");
  return commit?receipt(work.transaction(),((java.sql.Timestamp)rows.getFirst()[0]).toInstant(),work.lines()):null;
 }
 public List<String> pending() {
  return jdbc.queryForList("SELECT saga_id FROM checkout_sagas WHERE state IN ('RESERVING','COMMITTING','CANCELLING') AND next_attempt_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY next_attempt_at,saga_id LIMIT 20",String.class);
 }
 /** 功能：只有当前租约所有者能记录失败，退避和释放租约一次写入完成。 */
 public void failed(Work work,Exception error) {
  String message=error.getClass().getSimpleName()+": "+error.getMessage();
  jdbc.update("UPDATE checkout_sagas SET attempts=attempts+1,last_error=?,updated_at=now(),lease_owner=NULL,lease_until=NULL,next_attempt_at=now()+(LEAST(30,power(2,LEAST(attempts,5)))+random())*interval '1 second' WHERE saga_id=? AND lease_owner=? AND state IN ('RESERVING','COMMITTING','CANCELLING')",message.substring(0,Math.min(1000,message.length())),work.id(),work.owner());
 }
}
