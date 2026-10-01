package edu.cs6510.pipeline_server.persistence.entity;

import jakarta.persistence.*;

/** Immutable audit log; sequence allocation and basket update commit together. */
@Entity
@Table(name = "scan_events")
public class ScanEvent {
    @Id
    private long sequence;
    @Column(nullable = false, length = 32)
    private String sku;
    protected ScanEvent() {}
}
