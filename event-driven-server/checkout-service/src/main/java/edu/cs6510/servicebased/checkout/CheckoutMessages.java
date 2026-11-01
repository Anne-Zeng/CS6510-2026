package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.events.Request;
import edu.cs6510.events.Responses;
import edu.cs6510.servicebased.checkout.contract.CatalogItemResponse;
import edu.cs6510.servicebased.checkout.contract.ScanItemRequest;
import edu.cs6510.servicebased.checkout.contract.StartTransactionRequest;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox.PreparedScan;
import jakarta.validation.Validator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.Map;
//Kafka 业务入口，按 operation 分派请求
/** Kafka entry point for Gateway. Success waits for the actual local operation,
 * including durable Inventory confirmation when completing a transaction.
 */
@Component
public class CheckoutMessages {
 private final MessageBus bus;
 private final TransactionService checkout;
 private final Validator validator;
 private final OutboxStore outbox;
 private final CommandInbox inbox;
 private final ScanWriter scans;
 public CheckoutMessages(MessageBus bus,TransactionService checkout,Validator validator,OutboxStore outbox,CommandInbox inbox,ScanWriter scans) {
  this.bus=bus;this.checkout=checkout;this.validator=validator;this.outbox=outbox;this.inbox=inbox;this.scans=scans;
 }
 @EventListener(ApplicationReadyEvent.class)
 public void start() { bus.serve("checkout",this::handle); }//收到发给 Checkout 的请求时使用handle()方法处理
 Reply handle(Request request) {//按操作名分派业务处理
  return switch(request.operation()) {
   case "START_TRANSACTION" -> start(request);
   case "GET_TRANSACTION" -> Responses.call(()->checkout.getTransaction(request.key()));
   case "SCAN_ITEM" -> scan(request);
   case "COMPLETE_TRANSACTION" -> Responses.call(()->checkout.completeTransaction(request.key()));
   case "SCAN_CHECKPOINT" -> Responses.call(()->Map.of("totalScans",outbox.totalCount()));
   default -> Responses.call(()-> { throw new IllegalArgumentException("Unknown Checkout operation"); });
  };
 }
 private Reply scan(Request request) {
  try {
   CatalogItemResponse item;
   try { item=checkout.prepareScan(read(request.body(),ScanItemRequest.class)); }
   catch(RuntimeException failure) {
    // The catalog may have changed since the original successful command.
    // Resolve replays from durable storage even if that old SKU no longer exists.
    var replay=inbox.cached(request.id());
    if(replay!=null) return replay;
    throw failure;
   }
   return scans.scan(new PreparedScan(request.id(),request.key(),item));
  } catch(RuntimeException ex) {
   return Responses.call(()-> { throw ex; });
  }
 }
 private Reply start(Request request) {
  try { return inbox.start(request.id(),checkout.prepareStation(read(request.body(),StartTransactionRequest.class))); }
  catch(RuntimeException ex) { return Responses.call(()-> { throw ex; }); }
 }
 private <T> T read(String body,Class<T> type) {
  T value=Json.read(body,type);
  Responses.validate(validator,value);
  return value;
 }
}
