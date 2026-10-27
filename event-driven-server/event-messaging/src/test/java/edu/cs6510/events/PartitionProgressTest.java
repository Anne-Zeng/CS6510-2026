package edu.cs6510.events;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class PartitionProgressTest {
    @Test void outOfOrderRepliesCannotCommitAnUnfinishedCommand() {
        var progress = new PartitionProgress();
        progress.dispatched(10);
        progress.dispatched(11);
        progress.dispatched(12);
        progress.finished(12, true, 0);
        progress.finished(11, true, 0);
        assertEquals(-1, progress.completedOffset());
        assertEquals(1, progress.inFlight());
        progress.finished(10, true, 0);
        assertEquals(13, progress.completedOffset());
        assertEquals(0, progress.inFlight());
    }

    @Test void failedPublicationRetainsItsOffsetUntilAllWorkersFinishAndReplaySucceeds() {
        var progress = new PartitionProgress();
        for (long offset = 40; offset <= 43; offset++) progress.dispatched(offset);
        progress.finished(40, true, 0);
        progress.finished(41, false, 0);
        progress.finished(43, true, 0);
        assertEquals(41, progress.completedOffset());
        assertFalse(progress.retryReady(TimeUnit.SECONDS.toNanos(1)));
        assertThrows(IllegalStateException.class, progress::restart);
        progress.finished(42, true, 0);
        assertFalse(progress.retryReady(TimeUnit.MILLISECONDS.toNanos(499)));
        assertTrue(progress.retryReady(TimeUnit.MILLISECONDS.toNanos(500)));
        assertEquals(41, progress.restart());
        assertEquals(41, progress.completedOffset());
        for (long offset = 41; offset <= 43; offset++) progress.dispatched(offset);
        for (long offset = 41; offset <= 43; offset++) progress.finished(offset, true, 0);
        assertEquals(44, progress.completedOffset());
        assertFalse(progress.failed());
    }

    @Test void kafkaOffsetGapsDoNotPreventCommittingAllDeliveredRecords() {
        var progress = new PartitionProgress();
        progress.dispatched(10);
        progress.dispatched(15);
        progress.finished(15, true, 0);
        assertEquals(-1, progress.completedOffset());
        progress.finished(10, true, 0);
        assertEquals(16, progress.completedOffset());
    }
}
