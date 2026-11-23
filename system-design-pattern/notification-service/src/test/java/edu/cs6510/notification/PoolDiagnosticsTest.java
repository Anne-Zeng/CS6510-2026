package edu.cs6510.notification;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PoolDiagnosticsTest {
    @Test void recordsWaitersWithoutBorrowingConnections() throws Exception {
        var source = mock(HikariDataSource.class);
        var pool = mock(HikariPoolMXBean.class);
        when(source.getHikariPoolMXBean()).thenReturn(pool);
        when(source.getMaximumPoolSize()).thenReturn(8);
        when(pool.getActiveConnections()).thenReturn(8);
        when(pool.getThreadsAwaitingConnection()).thenReturn(25);
        var dispatch = mock(DispatchDatabase.class);
        when(dispatch.waitingThreads()).thenReturn(30);
        var values = new PoolDiagnostics(source, dispatch).snapshot();
        assertEquals(30, values.get("dispatchWaiting"));
        assertEquals(8, values.get("active"));
        assertEquals(25, values.get("waiting"));
        assertEquals(8, values.get("maximum"));
        verify(source, never()).getConnection();
    }

    @Test void handlesUninitializedAndOtherDataSources() {
        assertTrue(new PoolDiagnostics(mock(HikariDataSource.class), mock(DispatchDatabase.class)).snapshot().isEmpty());
        assertTrue(new PoolDiagnostics(mock(DataSource.class), mock(DispatchDatabase.class)).snapshot().isEmpty());
    }
}
