package edu.cs6510.layered_server.api;

import edu.cs6510.layered_server.contract.CatalogResponse;
import edu.cs6510.layered_server.transactions.CatalogService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/items")
public class CatalogController {
    private final CatalogService catalog;
    public CatalogController(CatalogService catalog) { this.catalog = catalog; }
    @GetMapping
    public CatalogResponse getCatalog() { return catalog.getCatalog(); }
}
