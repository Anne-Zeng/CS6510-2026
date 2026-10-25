package edu.cs6510.servicebased.catalog.persistence;

import edu.cs6510.servicebased.catalog.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

/** 功能：集中定义商品表查询，只供 Catalog 服务使用。 */

public interface CatalogItemRepository extends JpaRepository<CatalogItem, String> {
    // 功能：从数据库按 SKU 升序读取全部商品。
    List<CatalogItem> findAllByOrderBySkuAsc();
}
