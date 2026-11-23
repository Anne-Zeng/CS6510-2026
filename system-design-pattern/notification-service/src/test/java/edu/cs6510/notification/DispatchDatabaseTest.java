package edu.cs6510.notification;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DispatchDatabaseTest {
    @Test void boundsConcurrentCallsAndReleasesAllWaiters() throws Exception {
        var gate = new DispatchDatabase(mock(NotificationStore.class), 4, 8);
        var entered = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<Integer>>();
            try {
                for (int index = 0; index < 20; index++) tasks.add(workers.submit(() -> gate.execute(() -> {
                    maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out");
                        return 1;
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(failure);
                    } finally {
                        active.decrementAndGet();
                    }
                })));
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertEquals(4, active.get());
            } finally {
                release.countDown();
            }
            for (var task : tasks) assertEquals(1, task.get(5, TimeUnit.SECONDS));
            assertEquals(4, maximum.get());
            assertEquals(0, active.get());
        }
    }

    @Test void failuresAndInterruptedAcquisitionDoNotLeakPermits() throws Exception {
        var gate = new DispatchDatabase(mock(NotificationStore.class), 1, 8);
        assertThrows(IllegalStateException.class, () -> gate.execute(() -> { throw new IllegalStateException(); }));
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> gate.execute(() -> fail("Must not execute")));
        } finally {
            Thread.interrupted();
        }
        assertEquals(7, gate.execute(() -> 7));
    }

    @Test void delegatesDatabaseCallsAndSkipsEmptyWrites() throws Exception {
        var store = mock(NotificationStore.class);
        var gate = new DispatchDatabase(store, 4, 8);
        gate.acknowledgeBatch("alice", List.of());
        gate.sent(List.of());
        verifyNoInteractions(store);
        gate.pending("alice");
        gate.acknowledgeBatch("alice", List.of("id"));
        gate.sent(List.of("id"));
        gate.nextBatch("alice", List.of("sent"), List.of("ack"));
        verify(store).pending("alice");
        verify(store).acknowledgeBatch("alice", List.of("id"));
        verify(store).sent(List.of("id"));
        verify(store).dispatchBatch("alice", List.of("sent"), List.of("ack"));
        verifyNoMoreInteractions(store);
    }

    @Test void rejectsConfigurationThatConsumesReservedCapacity() {
        for (int limit : List.of(0, -1, 7, 8))
            assertThrows(IllegalArgumentException.class,
                    () -> new DispatchDatabase(mock(NotificationStore.class), limit, 8));
    }

    @Test void acknowledgementsArrivingDuringGateWaitJoinTheSendTransaction() throws Exception {
        verifyDrainTiming(true);
    }

    @Test void disabledOptimizationPreservesEarlyDrainForComparison() throws Exception {
        verifyDrainTiming(false);
    }

    private void verifyDrainTiming(boolean atGate) throws Exception {
        var store = mock(NotificationStore.class);
        var gate = new DispatchDatabase(store, 1, 8);
        org.springframework.test.util.ReflectionTestUtils.setField(gate, "ackDrainAtGate", atGate);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queue = new java.util.concurrent.ArrayBlockingQueue<String>(1024);
        var drains = new AtomicInteger();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var holder = workers.submit(() -> gate.execute(() -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
                return null;
            }));
            java.util.concurrent.Future<List<Protocol.Delivery>> dispatch = null;
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                dispatch = workers.submit(() -> gate.nextBatch("alice", List.of("sent"), () -> {
                    drains.incrementAndGet();
                    var identifiers = new java.util.ArrayList<String>();
                    queue.drainTo(identifiers);
                    return identifiers;
                }));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (gate.waitingThreads() == 0 && System.nanoTime() < deadline) Thread.sleep(1);
                assertEquals(1, gate.waitingThreads());
                assertEquals(atGate ? 0 : 1, drains.get());
                queue.add("sent");
            } finally {
                release.countDown();
            }
            holder.get(5, TimeUnit.SECONDS);
            assertNotNull(dispatch);
            dispatch.get(5, TimeUnit.SECONDS);
        }
        verify(store).dispatchBatch("alice", List.of("sent"), atGate ? List.of("sent") : List.of());
        assertEquals(1, drains.get());
        assertEquals(atGate ? 0 : 1, queue.size());
    }

    @Test void interruptedWaitDoesNotConsumeAcknowledgements() {
        var gate = new DispatchDatabase(mock(NotificationStore.class), 1, 8);
        org.springframework.test.util.ReflectionTestUtils.setField(gate, "ackDrainAtGate", true);
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> gate.nextBatch("alice", List.of(), () -> {
                fail("Must not drain before acquiring permit");
                return List.of();
            }));
        } finally {
            Thread.interrupted();
        }
    }

    @Test void drainFailureReleasesThePermit() throws Exception {
        var gate = new DispatchDatabase(mock(NotificationStore.class), 1, 8);
        org.springframework.test.util.ReflectionTestUtils.setField(gate, "ackDrainAtGate", true);
        assertThrows(IllegalStateException.class, () -> gate.nextBatch("alice", List.of(), () -> {
            throw new IllegalStateException("Drain failed");
        }));
        assertEquals(7, gate.execute(() -> 7));
    }
}
