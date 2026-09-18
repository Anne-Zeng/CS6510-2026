package edu.cs6510.monolith_server.analytics;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "scan_events")
public class ScanEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long sequenceNumber;

    @Column(nullable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private Instant scannedAt;

    protected ScanEvent() {
        // Required by JPA.
    }

    public ScanEvent(String sku, Instant scannedAt) {
        this.sku = sku;
        this.scannedAt = scannedAt;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getSku() {
        return sku;
    }

    public Instant getScannedAt() {
        return scannedAt;
    }
}