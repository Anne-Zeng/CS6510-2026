package edu.cs6510.servicebased.checkout;

import java.util.List;
import edu.cs6510.servicebased.checkout.contract.ReceiptLine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;

/** 功能：库存 HTTP 客户端。超时和 5xx 表示结果未知，协调者保留当前步骤重试。 */
@Component
public class InventoryClient {
 public record Item(String sku, int quantity) {}
 public record Request(List<Item> items) {}
 public record Reply(String transactionId, String status) {}
 public static class Rejected extends RuntimeException {}
 private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(InventoryClient.class);
 @Value("${checkout.inventory.performance-log:false}") private boolean performanceLog;
 private final java.util.concurrent.atomic.LongAdder calls=new java.util.concurrent.atomic.LongAdder(),waitNanos=new java.util.concurrent.atomic.LongAdder(),rpcNanos=new java.util.concurrent.atomic.LongAdder();
 private final RestClient client;
 private final HttpClient http;
 private final Semaphore inFlight;
 public InventoryClient(@Value("${inventory.base-url:http://localhost:8084}") String url,
     @Value("${checkout.inventory.max-in-flight:8}") int maximum) {
  if(maximum<1) throw new IllegalArgumentException("Invalid inventory concurrency");
  inFlight=new Semaphore(maximum,true);
  http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).version(HttpClient.Version.HTTP_1_1).build();
  var factory=new JdkClientHttpRequestFactory(http);factory.setReadTimeout(Duration.ofSeconds(3));
  client=RestClient.builder().baseUrl(url).requestFactory(factory).build();
 }
 /** 功能：一步扣减并确认整篮，返回 CONFIRMED；上一版两阶段预留遗留的记录返回 RESERVED，需再确认。 */
 public String reserve(String attemptId, List<ReceiptLine> lines) {
  try {
   var request=new Request(lines.stream().map(l->new Item(l.sku(),l.quantity())).toList());
   Reply result=invoke(()->client.post().uri("/internal/reservations/{id}",attemptId).body(request).retrieve().body(Reply.class));
   validate(result,attemptId,"RESERVED","CONFIRMED");
   return result.status();
  } catch(HttpClientErrorException.Conflict ex) { throw new Rejected(); }
 }
 public void confirm(String attemptId) { action(attemptId,"confirm","CONFIRMED"); }
 public void release(String attemptId) { action(attemptId,"release","CANCELLED"); }
 private void action(String id,String action,String expected) {
  var reply=invoke(()->client.post().uri("/internal/reservations/{id}/{action}",id,action).retrieve().body(Reply.class));
  validate(reply,id,expected);
 }
 /** 功能：仅远程库存调用占许可；Checkout 自己的 SQL 与状态计算不占许可。 */
 private <T> T invoke(Supplier<T> operation) {
  boolean acquired=false;
  long started=System.nanoTime(),acquiredAt=started;
  try {
   acquired=inFlight.tryAcquire(2,TimeUnit.SECONDS);
   if(!acquired) throw new IllegalStateException("Inventory calls busy; retry the same operation");
   acquiredAt=System.nanoTime();
   return operation.get();
  } catch(InterruptedException ex) {
   Thread.currentThread().interrupt();throw new IllegalStateException("Inventory call interrupted",ex);
  } finally {
   if(acquired) inFlight.release();
   if(performanceLog) {
    waitNanos.add(acquiredAt-started);rpcNanos.add(System.nanoTime()-acquiredAt);calls.increment();
    long count=calls.sum();
    if(count%1000==0) log.info("Inventory RPC timing: calls={} gateMeanMs={} rpcMeanMs={}",count,waitNanos.sum()/1e6/count,rpcNanos.sum()/1e6/count);
   }
  }
 }
 @PreDestroy public void close() { http.close(); }
 private void validate(Reply reply,String id,String... allowed) {
  if(reply==null || !id.equals(reply.transactionId()) || !List.of(allowed).contains(reply.status()))
   throw new IllegalStateException("Unexpected inventory acknowledgement");
 }
}
