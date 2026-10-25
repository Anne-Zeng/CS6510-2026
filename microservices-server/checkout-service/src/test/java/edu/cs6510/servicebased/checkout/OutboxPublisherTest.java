package edu.cs6510.servicebased.checkout;
import edu.cs6510.servicebased.checkout.persistence.OutboxStore;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.stream.IntStream;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
/** 功能：接收方未确认前不能删除或标记源事件；重复投递保持原事件身份。 */
class OutboxPublisherTest {
 OutboxStore store=mock(OutboxStore.class);
 AnalyticsClient client=mock(AnalyticsClient.class);
 OutboxPublisher publisher=new OutboxPublisher(store,client);
 List<OutboxStore.Event> events=List.of(new OutboxStore.Event("11111111-1111-1111-1111-111111111111","A","Apple"));
 @Test void failedDeliveryLeavesEventsPendingAndRetriesSameIdentity() {
  when(store.pending()).thenReturn(events);
  doThrow(new IllegalStateException("response lost")).doNothing().when(client).deliver(events);
  assertThrows(IllegalStateException.class,()->publisher.publishOnce());
  verify(store,never()).acknowledge(anyList());
  assertEquals(1,publisher.publishOnce());
  verify(client,times(2)).deliver(events);verify(store).acknowledge(events);
 }
 @Test void acknowledgementFollowsRemoteCommit() {
  when(store.pending()).thenReturn(events);
  publisher.publishOnce();var order=inOrder(client,store);
  order.verify(client).deliver(events);order.verify(store).acknowledge(events);
 }
 @Test void emptyOutboxMakesNoRemoteCall() {
  when(store.pending()).thenReturn(List.of());assertEquals(0,publisher.publishOnce());verifyNoInteractions(client);
 }
 @Test void backlogDrainsConsecutiveBatchesWithoutWaitingBetweenThem() {
  var full=IntStream.range(0,OutboxStore.BATCH_SIZE).mapToObj(n->new OutboxStore.Event("id-"+n,"A","Apple")).toList();
  when(store.pending()).thenReturn(full,full,events);
  publisher.publish();
  verify(store,times(2)).acknowledge(full);verify(store).acknowledge(events);
 }
 @Test void aLaterFailedBatchCannotBeAcknowledgedWithEarlierSuccesses() {
  var full=IntStream.range(0,OutboxStore.BATCH_SIZE).mapToObj(n->new OutboxStore.Event("id-"+n,"A","Apple")).toList();
  when(store.pending()).thenReturn(full,events);
  doThrow(new IllegalStateException("lost response")).when(client).deliver(events);
  publisher.publish();verify(store).acknowledge(full);verify(store,never()).acknowledge(events);
 }
}
