package edu.cs6510.monolith_server.catalog;

import edu.cs6510.monolith_server.catalog.api.CatalogItemResponse;
import edu.cs6510.monolith_server.catalog.api.CatalogResponse;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CatalogService {

    private final CatalogItemRepository catalogItemRepository;

    public CatalogService(CatalogItemRepository catalogItemRepository) {
        this.catalogItemRepository = catalogItemRepository;
    }

    public CatalogResponse getCatalog() {
        List<CatalogItemResponse> items = catalogItemRepository
                .findAllByOrderBySkuAsc()
                .stream()
                .map(item -> new CatalogItemResponse(
                        item.getSku(),
                        item.getName(),
                        item.getPrice()
                ))
                .toList();

        return new CatalogResponse(items);
    }
}