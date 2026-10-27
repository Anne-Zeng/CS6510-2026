package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.*;
import edu.cs6510.servicebased.checkout.contract.*;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox.PreparedScan;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CheckoutMessagesTest {
 MessageBus bus=mock(MessageBus.class);
 TransactionService service=mock(TransactionService.class);
 CommandInbox inbox=mock(CommandInbox.class);
 OutboxStore outbox=mock(OutboxStore.class);
 ScanWriter scans=mock(ScanWriter.class);
 ValidatorFactory validation=Validation.buildDefaultValidatorFactory();
 CheckoutMessages endpoint=new CheckoutMessages(bus,service,validation.getValidator(),outbox,inbox,scans);
 Request request(String operation,String body) { return new Request("request-1",operation,"transaction-1",body,"reply-topic"); }
 @AfterEach void close() { validation.close(); }
 @Test void replayedScanSurvivesCatalogItemRemoval() {
  var cached=new Reply(200,"{\"itemCount\":1}");
  when(service.prepareScan(new ScanItemRequest("A"))).thenThrow(new IllegalArgumentException("Unknown SKU"));
  when(inbox.cached("request-1")).thenReturn(cached);
  assertEquals(cached,endpoint.handle(request("SCAN_ITEM","{\"sku\":\"A\"}")));
  verifyNoInteractions(scans);
 }
 @Test void resolvesCatalogBeforeOneAtomicScanWithoutASeparateInboxRead() {
  var item=new CatalogItemResponse("A","Apple",BigDecimal.ONE);
  var result=new ScanResult("transaction-1","A","Apple",BigDecimal.ONE,1,BigDecimal.ONE);
  when(service.prepareScan(new ScanItemRequest("A"))).thenReturn(item);
  when(scans.scan(new PreparedScan("request-1","transaction-1",item))).thenReturn(new Reply(200,Json.write(result)));
  var response=endpoint.handle(request("SCAN_ITEM","{\"sku\":\"A\"}"));
  assertEquals(result,Json.read(response.body(),ScanResult.class));
  var order=inOrder(service,scans);
  order.verify(service).prepareScan(new ScanItemRequest("A"));
  order.verify(scans).scan(new PreparedScan("request-1","transaction-1",item));
  verify(inbox,never()).cached(anyString());
 }
 @Test void startValidatesAndNormalizesStationBeforeAtomicCommand() {
  when(service.prepareStation(new StartTransactionRequest(" station-1 "))).thenReturn("station-1");
  var response=new Reply(201,"{\"transactionId\":\"new-basket\"}");
  when(inbox.start("request-1","station-1")).thenReturn(response);
  assertEquals(response,endpoint.handle(request("START_TRANSACTION","{\"stationId\":\" station-1 \"}")));
  verify(service,never()).startTransaction(any());
 }
 @Test void validatesScanBeforeAnyBusinessWrite() {
  assertEquals(400,endpoint.handle(request("SCAN_ITEM","{\"sku\":\" \"}")).status());
  verifyNoInteractions(service,scans);
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
