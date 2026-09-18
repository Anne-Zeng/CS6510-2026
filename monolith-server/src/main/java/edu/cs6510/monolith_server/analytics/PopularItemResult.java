package edu.cs6510.monolith_server.analytics;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "popular_item_results")
public class PopularItemResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "window_id", nullable = false)
    private AnalyticsWindow window;

    @Column(nullable = false, length = 32)
    private String sku;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int scanCount;

    @Column(nullable = false)
    private int rank;

    protected PopularItemResult() {
        // Required by JPA.
    }

    public PopularItemResult(
            AnalyticsWindow window,
            String sku,
            String name,
            int scanCount,
            int rank
    ) {
        this.window = window;
        this.sku = sku;
        this.name = name;
        this.scanCount = scanCount;
        this.rank = rank;
    }

    public String getSku() {
        return sku;
    }

    public String getName() {
        return name;
    }

    public int getScanCount() {
        return scanCount;
    }

    public int getRank() {
        return rank;
    }
}