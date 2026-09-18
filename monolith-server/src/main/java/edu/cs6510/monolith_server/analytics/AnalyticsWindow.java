package edu.cs6510.monolith_server.analytics;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "analytics_windows")
public class AnalyticsWindow {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private int windowSize;

    @Column(nullable = false)
    private int slideInterval;

    @Column(nullable = false)
    private long windowStart;

    @Column(nullable = false, unique = true)
    private long windowEnd;

    @Column(nullable = false)
    private Instant computedAt;

    protected AnalyticsWindow() {
        // Required by JPA.
    }

    public AnalyticsWindow(
            int windowSize,
            int slideInterval,
            long windowStart,
            long windowEnd,
            Instant computedAt
    ) {
        this.windowSize = windowSize;
        this.slideInterval = slideInterval;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.computedAt = computedAt;
    }

    public Long getId() {
        return id;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public int getSlideInterval() {
        return slideInterval;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public Instant getComputedAt() {
        return computedAt;
    }
}