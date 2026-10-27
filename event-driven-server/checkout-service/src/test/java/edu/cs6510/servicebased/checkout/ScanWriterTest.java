package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Reply;
import edu.cs6510.servicebased.checkout.contract.CatalogItemResponse;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox;
import edu.cs6510.servicebased.checkout.persistence.CommandInbox.PreparedScan;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

class ScanWriterTest {
    private final CatalogItemResponse item=new CatalogItemResponse("A","Apple",new BigDecimal("1.25"));
    private PreparedScan scan(String id,String basket) { return new PreparedScan(id,basket,item); }
    private record Call(Thread thread,CompletableFuture<Reply> result) {}
    private Call submit(ScanWriter writer,PreparedScan scan) {
        var result=new CompletableFuture<Reply>();
        var thread=Thread.startVirtualThread(()-> {
            try { result.complete(writer.scan(scan)); }
            catch(Throwable failure) { result.completeExceptionally(failure); }
        });
        return new Call(thread,result);
    }
    private void queued(Call call) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
        while(call.thread().getState()!=Thread.State.TIMED_WAITING && !call.result().isDone() && System.nanoTime()<deadline)
            Thread.sleep(1);
        assertFalse(call.result().isDone(),"Scan should be waiting for its durable result");
        assertEquals(Thread.State.TIMED_WAITING,call.thread().getState());
    }

    @Test void returnsSuccessOnlyAfterTheDurableWriteCompletes() throws Exception {
        try(var harness=new Harness(512,6000)) {
            var first=harness.blockFirst();
            assertFalse(first.result().isDone());
            harness.release.countDown();
            assertEquals(200,first.result().get(3,TimeUnit.SECONDS).status());
        }
    }

    @Test void coalescesAlreadyWaitingIndependentBasketsWithoutAnExtraInboxRead() throws Exception {
        try(var harness=new Harness(512,6000)) {
            var first=harness.blockFirst();
            var a=submit(harness.writer,scan("a","basket-a"));queued(a);
            var b=submit(harness.writer,scan("b","basket-b"));queued(b);
            var c=submit(harness.writer,scan("c","basket-c"));queued(c);
            harness.release.countDown();
            for(var call:List.of(first,a,b,c)) assertEquals(200,call.result().get(3,TimeUnit.SECONDS).status());
            verify(harness.inbox).scanBatch(argThat(batch->batch.size()==3));
            verify(harness.inbox,never()).cached(anyString());
        }
    }

    @Test void repeatedRequestIdsAndBasketsNeverShareASqlBatch() throws Exception {
        try(var harness=new Harness(512,6000)) {
            var seen=new HashMap<String,Reply>();
            var counts=new HashMap<String,Integer>();
            when(harness.inbox.scan(anyString(),anyString(),any())).thenAnswer(invocation-> {
                String id=invocation.getArgument(0),basket=invocation.getArgument(1);
                if(id.equals("gate")) harness.awaitRelease();
                return seen.computeIfAbsent(id,key->new Reply(200,"{\"itemCount\":"+counts.merge(basket,1,Integer::sum)+"}"));
            });
            when(harness.inbox.scanBatch(anyList())).thenAnswer(invocation-> {
                List<PreparedScan> batch=invocation.getArgument(0);
                assertEquals(batch.size(),new HashSet<>(batch.stream().map(PreparedScan::requestId).toList()).size());
                assertEquals(batch.size(),new HashSet<>(batch.stream().map(PreparedScan::transactionId).toList()).size());
                var replies=new HashMap<String,Reply>();
                for(var scan:batch) replies.put(scan.requestId(),seen.computeIfAbsent(scan.requestId(),
                        key->new Reply(200,"{\"itemCount\":"+counts.merge(scan.transactionId(),1,Integer::sum)+"}")));
                return replies;
            });
            var first=harness.blockFirst();
            var a=submit(harness.writer,scan("a","basket-a"));queued(a);
            var b=submit(harness.writer,scan("b","basket-b"));queued(b);
            var nextA=submit(harness.writer,scan("next-a","basket-a"));queued(nextA);
            var replayA=submit(harness.writer,scan("a","basket-a"));queued(replayA);
            harness.release.countDown();
            for(var call:List.of(first,a,b,nextA,replayA)) assertEquals(200,call.result().get(3,TimeUnit.SECONDS).status());
            assertEquals(a.result().get(),replayA.result().get());
            assertEquals("{\"itemCount\":2}",nextA.result().get().body());
            assertEquals(2,counts.get("basket-a"));
        }
    }

    @Test void rolledBackBatchRetriesEachOriginalCommandAndPreservesIndividualErrors() throws Exception {
        try(var harness=new Harness(512,6000)) {
            when(harness.inbox.scanBatch(anyList())).thenThrow(new DuplicateKeyException("concurrent request ID"));
            var rejected=new ResponseStatusException(org.springframework.http.HttpStatus.CONFLICT,"Basket closed");
            when(harness.inbox.scan(eq("bad"),anyString(),any())).thenThrow(rejected);
            var first=harness.blockFirst();
            var good=submit(harness.writer,scan("good","basket-good"));queued(good);
            var bad=submit(harness.writer,scan("bad","basket-bad"));queued(bad);
            harness.release.countDown();
            assertEquals(200,first.result().get(3,TimeUnit.SECONDS).status());
            assertEquals(200,good.result().get(3,TimeUnit.SECONDS).status());
            assertEquals(409,bad.result().get(3,TimeUnit.SECONDS).status());
            verify(harness.inbox).scan("good","basket-good",item);
            verify(harness.inbox).scan("bad","basket-bad",item);
        }
    }

    @Test void fullQueueRejectsNewWorkWith503InsteadOfDroppingAcceptedWork() throws Exception {
        try(var harness=new Harness(2,6000)) {
            var first=harness.blockFirst();
            var a=submit(harness.writer,scan("a","basket-a"));queued(a);
            var b=submit(harness.writer,scan("b","basket-b"));queued(b);
            assertEquals(503,assertThrows(ResponseStatusException.class,
                    ()->harness.writer.scan(scan("full","basket-full"))).getStatusCode().value());
            harness.release.countDown();
            for(var call:List.of(first,a,b)) assertEquals(200,call.result().get(3,TimeUnit.SECONDS).status());
            verify(harness.inbox,never()).scan(eq("full"),anyString(),any());
        }
    }

    @Test void timeoutLeavesAcceptedWorkIntactAndItsStableIdCanBeReplayed() throws Exception {
        try(var harness=new Harness(512,40)) {
            var first=harness.blockFirst();
            var failure=assertThrows(java.util.concurrent.ExecutionException.class,()->first.result().get(3,TimeUnit.SECONDS));
            assertEquals(503,((ResponseStatusException)failure.getCause()).getStatusCode().value());
            harness.release.countDown();
            // A following request is processed only after the timed-out accepted
            // write finishes; use the same ID to resolve the stored outcome.
            assertEquals(200,harness.writer.scan(scan("gate","basket-gate")).status());
            verify(harness.inbox,times(2)).scan("gate","basket-gate",item);
        }
    }

    @Test void closingFailsPendingWorkAndRejectsNewCommands() throws Exception {
        try(var harness=new Harness(512,6000)) {
            harness.blockFirst();
            var pending=submit(harness.writer,scan("pending","basket-pending"));queued(pending);
            harness.writer.close();
            var failure=assertThrows(java.util.concurrent.ExecutionException.class,
                    ()->pending.result().get(3,TimeUnit.SECONDS));
            assertEquals(503,((ResponseStatusException)failure.getCause()).getStatusCode().value());
            assertEquals(503,assertThrows(ResponseStatusException.class,
                    ()->harness.writer.scan(scan("after-close","basket-after-close"))).getStatusCode().value());
            verify(harness.inbox,never()).scan(eq("pending"),anyString(),any());
        }
    }

    @Test void configurationCanDisableBatchingAndUseTheOriginalAtomicStatement() {
        var inbox=mock(CommandInbox.class);
        var reply=new Reply(200,"{}");
        when(inbox.scan("a","basket-a",item)).thenReturn(reply);
        try(var writer=new ScanWriter(inbox,false,false)) {
            assertEquals(reply,writer.scan(scan("a","basket-a")));
            verify(inbox).scan("a","basket-a",item);
            verify(inbox,never()).scanBatch(anyList());
        }
    }

    private final class Harness implements AutoCloseable {
        final CommandInbox inbox=mock(CommandInbox.class);
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        final ScanWriter writer;
        Harness(int capacity,long timeout) {
            when(inbox.scan(anyString(),anyString(),any())).thenAnswer(invocation-> {
                if(invocation.getArgument(0).equals("gate")) awaitRelease();
                return new Reply(200,"{}");
            });
            when(inbox.scanBatch(anyList())).thenAnswer(invocation-> {
                List<PreparedScan> batch=invocation.getArgument(0);
                var replies=new HashMap<String,Reply>();
                batch.forEach(scan->replies.put(scan.requestId(),new Reply(200,"{}")));
                return replies;
            });
            writer=new ScanWriter(inbox,true,false,capacity,timeout);
        }
        void awaitRelease() {
            entered.countDown();
            try { if(!release.await(3,TimeUnit.SECONDS)) throw new IllegalStateException("Release barrier timed out"); }
            catch(InterruptedException stopping) { Thread.currentThread().interrupt();throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Stopping"); }
        }
        Call blockFirst() throws Exception {
            var call=submit(writer,scan("gate","basket-gate"));
            assertTrue(entered.await(3,TimeUnit.SECONDS));return call;
        }
        @Override public void close() { release.countDown();writer.close(); }
    }
}
