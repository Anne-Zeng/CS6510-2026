package edu.cs6510.subscriber;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SubscriberMainTest {
    @Test void configurableThreadsAndMultipleSkus() {
        var options = SubscriberMain.parse(new String[]{"--subscribers", "100", "--skus", "A,B,A"});
        assertEquals(100, options.subscribers());
        assertEquals(2, options.skus().size());
    }
    @Test void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> SubscriberMain.parse(new String[]{"--subscribers", "101"}));
        assertThrows(IllegalArgumentException.class, () -> SubscriberMain.parse(new String[]{"--skus", "A,"}));
        assertThrows(IllegalArgumentException.class, () -> SubscriberMain.parse(new String[]{"--unknown", "1"}));
    }
}
