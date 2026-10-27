package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.events.Json;
import edu.cs6510.events.Reply;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CommandInboxTest {
 JdbcTemplate jdbc=mock(JdbcTemplate.class);
 PlatformTransactionManager manager=mock(PlatformTransactionManager.class);
 TransactionStatus transaction=mock(TransactionStatus.class);
 CommandInbox inbox=new CommandInbox(jdbc,new TransactionTemplate(manager));
 @SuppressWarnings("unchecked")
 void cachedReplies(List<Reply> replies) {
  when(jdbc.query(eq("SELECT status,body FROM command_results WHERE request_id=?"),any(RowMapper.class),eq("request-1")))
   .thenReturn(replies);
  when(manager.getTransaction(any())).thenReturn(transaction);
 }
 @Test void redeliveryReturnsCommittedResponseWithoutCallingBusinessOperation() {
  var committed=new Reply(201,"{\"transactionId\":\"existing\"}");
  cachedReplies(List.of(committed));
  Supplier<?> operation=mock(Supplier.class);
  assertEquals(committed,inbox.execute("request-1",201,operation));
  verifyNoInteractions(operation);
  verify(jdbc,never()).update(anyString(),any(Object[].class));
  verify(manager).commit(transaction);
 }
 @Test void databaseFailureRollsBackAndDoesNotCacheAFailedResult() {
  cachedReplies(List.of());
  assertThrows(DataAccessResourceFailureException.class,()->inbox.execute("request-1",200,()-> {
   throw new DataAccessResourceFailureException("database disconnected");
  }));
  verify(manager).rollback(transaction);verify(manager,never()).commit(any());
  verify(jdbc,never()).update(anyString(),any(Object[].class));
 }
 @Test void resultInsertFailureRollsBackTheBusinessTransaction() {
  cachedReplies(List.of());
  String body=Json.write(Map.of("itemCount",1));
  when(jdbc.update("INSERT INTO command_results(request_id,status,body) VALUES (?,?,?)","request-1",200,body))
   .thenThrow(new DataAccessResourceFailureException("result insert failed"));
  assertThrows(DataAccessResourceFailureException.class,
   ()->inbox.execute("request-1",200,()->Map.of("itemCount",1)));
  verify(manager).rollback(transaction);verify(manager,never()).commit(any());
 }
}
