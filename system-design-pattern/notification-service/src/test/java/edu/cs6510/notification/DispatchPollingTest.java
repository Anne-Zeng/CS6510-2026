package edu.cs6510.notification;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DispatchPollingTest {
    @Test void idleBackoffIsBoundedAndResetsOnWork() {
        assertEquals(400, SubscriberHandler.nextIdleDelay(200, false, 1000));
        assertEquals(800, SubscriberHandler.nextIdleDelay(400, false, 1000));
        assertEquals(1000, SubscriberHandler.nextIdleDelay(800, false, 1000));
        assertEquals(1000, SubscriberHandler.nextIdleDelay(1000, false, 1000));
        assertEquals(200, SubscriberHandler.nextIdleDelay(1000, true, 1000));
        assertEquals(200, SubscriberHandler.nextIdleDelay(200, false, 200));
    }

    @Test void validatesPollingBounds() {
        for (int invalid : new int[]{0, 199, 1001})
            assertThrows(IllegalArgumentException.class, () ->
                    new SubscriberHandler(mock(NotificationStore.class), mock(DispatchDatabase.class), invalid));
    }
}
