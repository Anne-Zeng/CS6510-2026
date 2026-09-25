package edu.cs6510.layered_server.contract;

import java.util.List;

public record CatalogResponse(
        List<CatalogItemResponse> items
) {
}
