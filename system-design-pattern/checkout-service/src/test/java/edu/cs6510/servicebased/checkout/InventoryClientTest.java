package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.events.Reply;
import edu.cs6510.servicebased.checkout.contract.ReceiptLine;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InventoryClientTest {
 MessageBus bus=mock(MessageBus.class);
 InventoryClient client=new InventoryClient(bus,2);
 List<ReceiptLine> lines=List.of(new ReceiptLine("A","Apple",BigDecimal.ONE,2));
 @Test void sendsStableSagaIdAndReadsDurableConfirmation() {
  when(bus.request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),anyString()))
   .thenReturn(new Reply(200,"{\"transactionId\":\"attempt\",\"status\":\"CONFIRMED\"}"));
  assertEquals("CONFIRMED",client.reserve("attempt",lines));
  var body=org.mockito.ArgumentCaptor.forClass(String.class);
  verify(bus).request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),body.capture());
  var request=Json.read(body.getValue(),InventoryClient.ReserveRequest.class);
  assertEquals(List.of(new InventoryClient.Item("A",2)),request.items());
 }
 @Test void onlyDefinitiveConflictChoosesSagaCancellation() {
  when(bus.request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),anyString()))
   .thenReturn(new Reply(409,"{}"));
  assertThrows(InventoryClient.Rejected.class,()->client.reserve("attempt",lines));
 }
 @Test void timeoutKeepsOutcomeUnknownForIdempotentRetry() {
  when(bus.request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),anyString()))
   .thenThrow(new IllegalStateException("broker response timeout"))
   .thenReturn(new Reply(200,"{\"transactionId\":\"attempt\",\"status\":\"CONFIRMED\"}"));
  assertThrows(IllegalStateException.class,()->client.reserve("attempt",lines));
  assertEquals("CONFIRMED",client.reserve("attempt",lines));
  verify(bus,times(2)).request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),anyString());
 }
 @Test void cannotFinishSagaWithAnotherAttemptsOutcome() {
  when(bus.request("inventory","INVENTORY_CONFIRM","attempt","{}"))
   .thenReturn(new Reply(200,"{\"transactionId\":\"different\",\"status\":\"CONFIRMED\"}"));
  assertThrows(IllegalStateException.class,()->client.confirm("attempt"));
 }
 @Test void upstreamFailureIsNotARejection() {
  when(bus.request(eq("inventory"),eq("INVENTORY_RESERVE"),eq("attempt"),anyString()))
   .thenReturn(new Reply(503,"{}"));
  assertThrows(IllegalStateException.class,()->client.reserve("attempt",lines));
 }
}
