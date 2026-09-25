package edu.cs6510.layered_server.contract;

public record PopularItem(
        String sku,
        String name,
        int scanCount,
        int rank
) {
}
