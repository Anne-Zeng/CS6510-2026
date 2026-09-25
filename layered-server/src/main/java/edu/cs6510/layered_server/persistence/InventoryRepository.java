package edu.cs6510.layered_server.persistence;

import edu.cs6510.layered_server.persistence.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface InventoryRepository extends JpaRepository<Inventory, String> {
    @Modifying(flushAutomatically = true)
    @Query("""
            update Inventory i
            set i.lowStockTriggeredAt = case
                    when i.currentStock - :quantity < :threshold
                    then coalesce(i.lowStockTriggeredAt, :now)
                    else i.lowStockTriggeredAt end,
                i.currentStock = i.currentStock - :quantity
            where i.sku = :sku and i.currentStock >= :quantity and :quantity > 0
            """)
    int decrementIfEnough(@Param("sku") String sku, @Param("quantity") int quantity,
                          @Param("threshold") int threshold, @Param("now") Instant now);

    interface LowStockRow {
        String getSku();
        String getName();
        int getCurrentStock();
        Instant getTriggeredAt();
    }

    @Query("""
            select i.sku as sku, c.name as name, i.currentStock as currentStock,
                   i.lowStockTriggeredAt as triggeredAt
            from Inventory i join CatalogItem c on c.sku = i.sku
            where i.currentStock < :threshold
            order by i.currentStock asc, i.sku asc
            """)
    List<LowStockRow> findLowStock(@Param("threshold") int threshold);
}
