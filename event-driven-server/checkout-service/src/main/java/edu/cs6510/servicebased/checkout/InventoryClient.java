package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import java.util.List;
import edu.cs6510.servicebased.checkout.contract.ReceiptLine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Inventory command/result messages travel through Kafka. A timeout is an unknown
 * result: the durable saga retries with the same attempt ID rather than cancelling.
 */
@Component
public class InventoryClient {
 public record Item(String sku,int quantity) {}
 public record ReserveRequest(List<Item> items) {}
 public record Outcome(String transactionId,String status) {}
 public static class Rejected extends RuntimeException {}
 private final MessageBus bus;
 private final Semaphore inFlight;
 public InventoryClient(MessageBus bus,@Value("${checkout.inventory.max-in-flight:64}") int maximum) {
  if(maximum<1) throw new IllegalArgumentException("Invalid inventory concurrency");
  this.bus=bus;inFlight=new Semaphore(maximum,true);
 }
 /** Saga ID is both the Kafka partition key and Inventory's idempotency key. */
 public String reserve(String attemptId,List<ReceiptLine> lines) {
  var request=new ReserveRequest(lines.stream().map(line->new Item(line.sku(),line.quantity())).toList());
  var response=invoke(()->bus.request("inventory","INVENTORY_RESERVE",attemptId,Json.write(request)));
  if(response.status()==409) throw new Rejected();
  var outcome=readSuccess(response);
  validate(outcome,attemptId,"RESERVED","CONFIRMED");
  return outcome.status();
 }
 public void confirm(String attemptId) { action(attemptId,"INVENTORY_CONFIRM","CONFIRMED"); }
 public void release(String attemptId) { action(attemptId,"INVENTORY_RELEASE","CANCELLED"); }
 private void action(String id,String operation,String expected) {
  var response=invoke(()->bus.request("inventory",operation,id,"{}"));
  validate(readSuccess(response),id,expected);
 }
 private Outcome readSuccess(edu.cs6510.events.Reply response) {
  if(response.status()!=200) throw new IllegalStateException("Inventory returned "+response.status()+"; retry the same operation");
  return Json.read(response.body(),Outcome.class);
 }
 /** Waiting for a Kafka outcome holds a permit, never a database connection. */
 private <T> T invoke(Supplier<T> operation) {
  boolean acquired=false;
  try {
   acquired=inFlight.tryAcquire(2,TimeUnit.SECONDS);
   if(!acquired) throw new IllegalStateException("Inventory commands busy; retry the same operation");
   return operation.get();
  } catch(InterruptedException ex) {
   Thread.currentThread().interrupt();throw new IllegalStateException("Inventory command interrupted",ex);
  } finally {
   if(acquired) inFlight.release();
  }
 }
 private void validate(Outcome outcome,String id,String... allowed) {
  if(outcome==null || !id.equals(outcome.transactionId()) || !List.of(allowed).contains(outcome.status()))
   throw new IllegalStateException("Unexpected inventory outcome");
 }
}
