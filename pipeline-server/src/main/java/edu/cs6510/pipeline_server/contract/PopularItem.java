package edu.cs6510.pipeline_server.contract;

public record PopularItem(
        String sku,
        String name,
        int scanCount,
        int rank
) {
}
