package edu.cs6510.monolith_server.catalog.api;

import java.util.List;

public record CatalogResponse(
        List<CatalogItemResponse> items
) {
}