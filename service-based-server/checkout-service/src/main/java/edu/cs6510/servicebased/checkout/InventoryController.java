package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.shared.contract.LowStockResponse;
import org.springframework.web.bind.annotation.*;

/** 功能：对外提供低库存查询；库存修改仅由本地结账流程执行。 */
@RestController
@RequestMapping("/inventory")
public class InventoryController {
    private final InventoryService inventory;

    // 功能：注入本进程的库存业务对象。
    public InventoryController(InventoryService inventory) { this.inventory = inventory; }

    // 功能：GET 按默认或指定阈值查询低库存商品。
    @GetMapping("/low-stock")
    public LowStockResponse getLowStock(@RequestParam(required = false) Integer threshold) {
        return inventory.getLowStock(threshold);
    }
}
