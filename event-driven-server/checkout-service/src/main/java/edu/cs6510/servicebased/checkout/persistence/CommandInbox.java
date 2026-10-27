package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.events.Json;
import edu.cs6510.events.Reply;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.function.Supplier;

/** Kafka may redeliver a command after the business commit but before its reply
 * or offset commit. Store the result atomically with START/SCAN business writes.
 * The advisory lock serializes duplicate request IDs, including separate replicas.
 */
@Repository
public class CommandInbox {
 private final JdbcTemplate jdbc;
 private final TransactionTemplate transactions;
 public CommandInbox(JdbcTemplate jdbc,TransactionTemplate transactions) {
  this.jdbc=jdbc;this.transactions=transactions;
 }
 public Reply cached(String id) {
  var replies=jdbc.query("SELECT status,body FROM command_results WHERE request_id=?",
    (row,index)->new Reply(row.getInt("status"),row.getString("body")),id);
  return replies.isEmpty()?null:replies.getFirst();
 }
 public Reply execute(String id,int status,Supplier<?> operation) {
  return transactions.execute(transaction->{
   jdbc.query("SELECT pg_advisory_xact_lock(hashtextextended(?,0))",row->{},id);
   var existing=cached(id);
   if(existing!=null) return existing;
   var reply=new Reply(status,Json.write(operation.get()));
   jdbc.update("INSERT INTO command_results(request_id,status,body) VALUES (?,?,?)",id,reply.status(),reply.body());
   return reply;
  });
 }
}
