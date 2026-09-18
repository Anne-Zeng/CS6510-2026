package edu.cs6510.monolith_server.inventory;

import edu.cs6510.monolith_server.inventory.api.LowStockResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/low-stock")
    public ResponseEntity<LowStockResponse> getLowStock(
            @RequestParam(required = false) Integer threshold
    ) {
        return ResponseEntity.ok(
                inventoryService.getLowStock(threshold)
        );
    }
}