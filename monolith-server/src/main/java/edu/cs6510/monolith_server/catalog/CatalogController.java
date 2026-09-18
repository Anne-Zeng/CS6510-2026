package edu.cs6510.monolith_server.catalog;

import edu.cs6510.monolith_server.catalog.api.CatalogResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/items")
public class CatalogController {

    private final CatalogService catalogService;

    public CatalogController(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GetMapping
    public ResponseEntity<CatalogResponse> getCatalog() {
        return ResponseEntity.ok(catalogService.getCatalog());
    }
}