package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.*;
import edu.cs6510.servicebased.checkout.contract.*;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CheckoutMessagesTest {
 MessageBus bus=mock(MessageBus.class);
 TransactionService service=mock(TransactionService.class);
 CommandInbox inbox=mock(CommandInbox.class);
 OutboxStore outbox=mock(OutboxStore.class);
 ValidatorFactory validation=Validation.buildDefaultValidatorFactory();
 CheckoutMessages endpoint=new CheckoutMessages(bus,service,validation.getValidator(),outbox,inbox);
 Request request(String operation,String body) { return new Request("request-1",operation,"transaction-1",body,"reply-topic"); }
 @AfterEach void close() { validation.close(); }
 @Test void replayedScanReturnsOriginalResponseWithoutResolvingCatalogOrWriting() {
  var cached=new Reply(200,"{\"itemCount\":1}");
  when(inbox.cached("request-1")).thenReturn(cached);
  assertEquals(cached,endpoint.handle(request("SCAN_ITEM","{\"sku\":\"A\"}")));
  verifyNoInteractions(service);
  verify(inbox,never()).execute(anyString(),anyInt(),any());
 }
 @Test void resolvesCatalogBeforeStartingDeduplicatedLocalScan() {
  var item=new CatalogItemResponse("A","Apple",BigDecimal.ONE);
  var result=new ScanResult("transaction-1","A","Apple",BigDecimal.ONE,1,BigDecimal.ONE);
  when(service.prepareScan(new ScanItemRequest("A"))).thenReturn(item);
  when(service.scanPrepared("transaction-1",item)).thenReturn(result);
  when(inbox.execute(eq("request-1"),eq(200),any())).thenAnswer(invocation->{
   Supplier<?> action=invocation.getArgument(2);return new Reply(200,Json.write(action.get()));
  });
  var response=endpoint.handle(request("SCAN_ITEM","{\"sku\":\"A\"}"));
  assertEquals(result,Json.read(response.body(),ScanResult.class));
  var order=inOrder(service,inbox);
  order.verify(inbox).cached("request-1");
  order.verify(service).prepareScan(new ScanItemRequest("A"));
  order.verify(inbox).execute(eq("request-1"),eq(200),any());
  order.verify(service).scanPrepared("transaction-1",item);
 }
 @Test void validatesScanBeforeAnyBusinessWrite() {
  assertEquals(400,endpoint.handle(request("SCAN_ITEM","{\"sku\":\" \"}")).status());
  verifyNoInteractions(service);verify(inbox,never()).execute(anyString(),anyInt(),any());
 }
 @Test void completeUsesDurableSagaWithoutHoldingAnInboxTransaction() {
  endpoint.handle(request("COMPLETE_TRANSACTION","{}"));
  verify(service).completeTransaction("transaction-1");verifyNoInteractions(inbox);
 }
 @Test void checkpointIncludesAllDurableScans() {
  when(outbox.totalCount()).thenReturn(1234L);
  assertEquals("{\"totalScans\":1234}",endpoint.handle(request("SCAN_CHECKPOINT","{}")).body());
 }
}
