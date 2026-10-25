package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.checkout.persistence.SagaStore;
import edu.cs6510.servicebased.checkout.persistence.SagaStore.Work;
import edu.cs6510.servicebased.checkout.contract.*;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.ResourceAccessException;
import java.util.List;
import java.time.Instant;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 功能：验证冻结上下文复用、一步扣减后直接完成、持久化取消决定顺序、失败释放及恢复幂等，不依赖真实数据库。 */
class CheckoutCoordinatorTest {
 SagaStore store=mock(SagaStore.class);
 InventoryClient inventory=mock(InventoryClient.class);
 CheckoutCoordinator coordinator=new CheckoutCoordinator(store,inventory);
 final Instant now=Instant.parse("2026-10-24T00:00:00Z");
 final List<ReceiptLine> lines=List.of(new ReceiptLine("A","Apple",BigDecimal.ONE,1));
 Work work(String state) { return new Work("attempt","tx",state,"owner",new TransactionResponse("tx","station","OPEN",1,BigDecimal.ONE,now),lines); }
 Receipt receipt() { return new Receipt("tx","station",1,BigDecimal.ONE,now,now,lines); }
 @Test void resumesDurableCommitWithoutReservingOrReleasing() {
  when(store.claim("attempt")).thenReturn(work("COMMITTING"));
  coordinator.drive("attempt");
  verify(inventory).confirm("attempt");verify(inventory,never()).reserve(anyString(),anyList());
  verify(inventory,never()).release(anyString());verify(store).finish(work("COMMITTING"));
 }
 @Test void repeatsConfirmationWhenAcknowledgementWasLost() {
  when(store.claim("attempt")).thenReturn(work("COMMITTING"));
  doThrow(new ResourceAccessException("lost response")).doNothing().when(inventory).confirm("attempt");
  assertThrows(ResourceAccessException.class,()->coordinator.drive("attempt"));
  verify(store,never()).finish(any());verify(store).failed(eq(work("COMMITTING")),any());
  coordinator.drive("attempt");verify(inventory,times(2)).confirm("attempt");
  verify(store).finish(work("COMMITTING"));verify(inventory,never()).release(anyString());
 }
 @Test void reserveTimeoutCannotChooseCancellation() {
  when(store.claim("attempt")).thenReturn(work("RESERVING"));
  doThrow(new ResourceAccessException("timeout")).when(inventory).reserve("attempt",lines);
  assertThrows(ResourceAccessException.class,()->coordinator.drive("attempt"));
  verify(store,never()).decide(any(),anyString());verify(inventory,never()).release(anyString());
  verify(store).failed(eq(work("RESERVING")),any());
 }
 @Test void definitiveRejectionPersistsCancelBeforeRelease() {
  when(store.claim("attempt")).thenReturn(work("RESERVING"));
  when(store.decide(work("RESERVING"),"CANCELLING")).thenReturn(work("CANCELLING"));
  doThrow(new InventoryClient.Rejected()).when(inventory).reserve("attempt",lines);
  coordinator.drive("attempt");var order=inOrder(store,inventory);
  order.verify(store).decide(work("RESERVING"),"CANCELLING");order.verify(inventory).release("attempt");
  order.verify(store).finish(work("CANCELLING"));verify(inventory,never()).confirm(anyString());
 }
 @Test void startupRecoveryResumesCancellation() {
  when(store.pending()).thenReturn(List.of("attempt"));when(store.claim("attempt")).thenReturn(work("CANCELLING"));
  coordinator.recover();verify(inventory).release("attempt");verify(store).finish(work("CANCELLING"));
 }
 @Test void busyLeaseDoesNotSendDuplicateInventoryCommands() {
  coordinator.drive("attempt");verifyNoInteractions(inventory);verify(store,never()).failed(any(),any());
 }
 @Test void normalCompleteDebitsOnceThenFinishesWithoutExtraDecisionOrConfirmation() {
  when(store.begin("tx")).thenReturn(new SagaStore.Start(work("RESERVING"),null));
  when(inventory.reserve("attempt",lines)).thenReturn("CONFIRMED");
  when(store.finish(work("RESERVING"))).thenReturn(receipt());
  assertEquals(receipt(),coordinator.complete("tx"));
  verify(store).begin("tx");verify(store,never()).claim(anyString());
  var order=inOrder(store,inventory);
  order.verify(inventory).reserve("attempt",lines);order.verify(store).finish(work("RESERVING"));
  verify(store,never()).decide(any(),anyString());verify(inventory,never()).confirm(anyString());
 }
 @Test void lostDebitAcknowledgementIsRetriedIdempotentlyThenFinished() {
  when(store.claim("attempt")).thenReturn(work("RESERVING"));
  doThrow(new ResourceAccessException("lost response")).doReturn("CONFIRMED").when(inventory).reserve("attempt",lines);
  assertThrows(ResourceAccessException.class,()->coordinator.drive("attempt"));
  verify(store,never()).finish(any());verify(store).failed(eq(work("RESERVING")),any());
  coordinator.drive("attempt");verify(inventory,times(2)).reserve("attempt",lines);
  verify(store).finish(work("RESERVING"));verify(store,never()).decide(any(),anyString());
 }
 @Test void legacyTwoPhaseReservationIsConfirmedBeforeFinishing() {
  when(store.claim("attempt")).thenReturn(work("RESERVING"));
  when(inventory.reserve("attempt",lines)).thenReturn("RESERVED");
  coordinator.drive("attempt");
  var order=inOrder(store,inventory);
  order.verify(inventory).reserve("attempt",lines);order.verify(inventory).confirm("attempt");
  order.verify(store).finish(work("RESERVING"));
 }
 @Test void completedTransactionReturnsPersistedReceiptWithoutInventoryCalls() {
  when(store.begin("tx")).thenReturn(new SagaStore.Start(null,receipt()));
  assertEquals(receipt(),coordinator.complete("tx"));verifyNoInteractions(inventory);
 }
}
