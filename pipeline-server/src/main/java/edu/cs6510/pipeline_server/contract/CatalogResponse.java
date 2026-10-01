package edu.cs6510.pipeline_server.contract;

import java.util.List;

public record CatalogResponse(
        List<CatalogItemResponse> items
) {
}
