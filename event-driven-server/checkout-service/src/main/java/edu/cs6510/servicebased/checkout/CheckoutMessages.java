package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.events.Request;
import edu.cs6510.events.Responses;
import edu.cs6510.servicebased.checkout.contract.ScanItemRequest;
import edu.cs6510.servicebased.checkout.contract.StartTransactionRequest;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import jakarta.validation.Validator;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.function.Supplier;

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
 public CheckoutMessages(MessageBus bus,TransactionService checkout,Validator validator,OutboxStore outbox,CommandInbox inbox) {
  this.bus=bus;this.checkout=checkout;this.validator=validator;this.outbox=outbox;this.inbox=inbox;
 }
 @EventListener(ApplicationReadyEvent.class)
 public void start() { bus.serve("checkout",this::handle); }
 Reply handle(Request request) {
  return switch(request.operation()) {
   case "START_TRANSACTION" -> command(request,201,()->checkout.startTransaction(read(request.body(),StartTransactionRequest.class)));
   case "GET_TRANSACTION" -> Responses.call(()->checkout.getTransaction(request.key()));
   case "SCAN_ITEM" -> scan(request);
   case "COMPLETE_TRANSACTION" -> Responses.call(()->checkout.completeTransaction(request.key()));
   case "SCAN_CHECKPOINT" -> Responses.call(()->Map.of("totalScans",outbox.totalCount()));
   default -> Responses.call(()-> { throw new IllegalArgumentException("Unknown Checkout operation"); });
  };
 }
 private Reply scan(Request request) {
  try {
   var cached=inbox.cached(request.id());
   if(cached!=null) return cached;
   var item=checkout.prepareScan(read(request.body(),ScanItemRequest.class));
   return inbox.execute(request.id(),200,()->checkout.scanPrepared(request.key(),item));
  } catch(RuntimeException ex) {
   return Responses.call(()-> { throw ex; });
  }
 }
 private Reply command(Request request,int status,Supplier<?> operation) {
  try { return inbox.execute(request.id(),status,operation); }
  catch(RuntimeException ex) { return Responses.call(()-> { throw ex; }); }
 }
 private <T> T read(String body,Class<T> type) {
  T value=Json.read(body,type);
  Responses.validate(validator,value);
  return value;
 }
}
