package edu.cs6510.pipeline_server.api;

import edu.cs6510.pipeline_server.contract.LowStockResponse;
import edu.cs6510.pipeline_server.transactions.InventoryService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/inventory")
public class InventoryController {
    private final InventoryService inventory;
    public InventoryController(InventoryService inventory) { this.inventory = inventory; }
    @GetMapping("/low-stock")
    public LowStockResponse getLowStock(@RequestParam(required = false) Integer threshold) {
        return inventory.getLowStock(threshold);
    }
}
