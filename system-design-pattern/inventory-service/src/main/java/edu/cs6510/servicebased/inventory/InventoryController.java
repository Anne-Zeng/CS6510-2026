package edu.cs6510.servicebased.inventory;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.Map;
/** 功能：公开低库存查询；internal 接口供后续 Checkout 协调者调用。 */
@RestController
public class InventoryController {
 private final InventoryService service;
 private final JdbcTemplate jdbc;
 public InventoryController(InventoryService service, JdbcTemplate jdbc) { this.service=service; this.jdbc=jdbc; }
 @GetMapping("/health") public Map<String,String> health() {
  jdbc.queryForObject("SELECT count(*) FROM inventory",Long.class);
  return Map.of("status","UP","service","inventory-service");
 }
 @GetMapping("/inventory/low-stock") public InventoryService.LowStock lowStock(@RequestParam(required=false) Integer threshold) { return service.lowStock(threshold); }
 @PostMapping("/internal/reservations/{id}") public InventoryService.Reservation reserve(@PathVariable String id,@Valid @RequestBody InventoryService.Request request) { return service.reserve(id,request); }
 @GetMapping("/internal/reservations/{id}") public InventoryService.Reservation get(@PathVariable String id) { return service.get(id); }
 @PostMapping("/internal/reservations/{id}/confirm") public InventoryService.Reservation confirm(@PathVariable String id) { return service.confirm(id); }
 @PostMapping("/internal/reservations/{id}/release") public InventoryService.Reservation release(@PathVariable String id) { return service.release(id); }
}
