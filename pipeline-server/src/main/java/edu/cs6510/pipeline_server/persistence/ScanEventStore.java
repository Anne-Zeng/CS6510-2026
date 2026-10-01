package edu.cs6510.pipeline_server.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

/** SQL and ordering belong to persistence, not to the window algorithm. */
@Repository
public class ScanEventStore {
    private final JdbcTemplate jdbc;
    public ScanEventStore(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String sku) {
        // Serialize sequence allocation until commit. Unlike an identity sequence,
        // rollback creates no gaps and later events cannot commit ahead of earlier ones.
        // The lock is database-local, so isolated benchmark databases do not contend.
        jdbc.execute("SELECT pg_advisory_xact_lock(6510, 1)");
        jdbc.update("INSERT INTO scan_events(sequence, sku) "
                + "SELECT COALESCE(MAX(sequence), 0) + 1, ? FROM scan_events", sku);
    }

    public long count() {
        return jdbc.queryForObject("SELECT COALESCE(MAX(sequence), 0) FROM scan_events", Long.class);
    }

    public List<String> between(long first, long last) {
        return jdbc.queryForList("SELECT sku FROM scan_events WHERE sequence BETWEEN ? AND ? "
                + "ORDER BY sequence", String.class, first, last);
    }
}
