package edu.cs6510.monolith_server.analytics.api;

public record PopularItem(
        String sku,
        String name,
        int scanCount,
        int rank
) {
}