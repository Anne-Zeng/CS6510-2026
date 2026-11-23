package edu.cs6510.events;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JsonTest {
 public record Line(String sku,BigDecimal price,int quantity) {}
 public record Receipt(String transactionId,Instant completedAt,List<Line> items) {}
 @Test void preservesRecordFieldsMoneyPrecisionAndJavaTimeAcrossTheMessageBoundary() {
  var receipt=new Receipt("tx-1",Instant.parse("2026-10-27T01:02:03.123456Z"),
    List.of(new Line("A",new BigDecimal("19.95"),2),new Line("B",new BigDecimal("0.10"),1)));
  assertEquals(receipt,Json.read(Json.write(receipt),Receipt.class));
 }
 @Test void requestEnvelopeRetainsIndependentRequestAndBusinessIdentities() {
  var request=new Request("command-1","SCAN_ITEM","transaction-2","{\"sku\":\"A\"}","test.gateway.replies");
  assertEquals(request,Json.read(Json.write(request),Request.class));
 }
 @Test void malformedInputCannotBecomeASuccessfulCommand() {
  assertThrows(tools.jackson.core.JacksonException.class,()->Json.read("{broken",Request.class));
 }
}
