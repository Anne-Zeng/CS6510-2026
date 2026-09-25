package edu.cs6510.layered_server.transactions;

import edu.cs6510.layered_server.contract.CatalogItemResponse;
import edu.cs6510.layered_server.contract.CatalogResponse;
import edu.cs6510.layered_server.persistence.CatalogCache;
import org.springframework.stereotype.Service;

@Service
public class CatalogService {
    private final CatalogCache catalog;

    public CatalogService(CatalogCache catalog) { this.catalog = catalog; }

    /** Served from the cached snapshot, so listing 2,000 items costs no query. */
    public CatalogResponse getCatalog() {
        return new CatalogResponse(catalog.findAllOrderedBySku().stream()
                .map(item -> new CatalogItemResponse(item.getSku(), item.getName(), item.getPrice()))
                .toList());
    }
}
