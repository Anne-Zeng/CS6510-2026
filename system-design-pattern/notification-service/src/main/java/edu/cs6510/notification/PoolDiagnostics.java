package edu.cs6510.notification;

import com.zaxxer.hikari.HikariDataSource;
import edu.cs6510.events.Json;
import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "notification.stage-timing", havingValue = "true")
public class PoolDiagnostics {
    private static final Logger log = LoggerFactory.getLogger(PoolDiagnostics.class);
    private final DataSource dataSource;
    private final DispatchDatabase dispatchDatabase;

    public PoolDiagnostics(DataSource dataSource, DispatchDatabase dispatchDatabase) {
        this.dataSource = dataSource;
        this.dispatchDatabase = dispatchDatabase;
    }

    Map<String, Object> snapshot() {
        if (!(dataSource instanceof HikariDataSource hikari)) return Map.of();
        var pool = hikari.getHikariPoolMXBean();
        if (pool == null) return Map.of();
        return Map.of("sampledAt", Instant.now(), "active", pool.getActiveConnections(),
                "idle", pool.getIdleConnections(), "total", pool.getTotalConnections(),
                "waiting", pool.getThreadsAwaitingConnection(), "maximum", hikari.getMaximumPoolSize(),
                "dispatchWaiting", dispatchDatabase.waitingThreads());
    }

    @Scheduled(fixedDelay = 1000)
    public void sample() {
        var values = snapshot();
        if (!values.isEmpty()) log.info("NOTIFICATION_POOL_TIMING {}", Json.write(values));
    }
}
