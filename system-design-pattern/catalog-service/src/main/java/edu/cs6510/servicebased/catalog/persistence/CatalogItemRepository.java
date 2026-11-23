package edu.cs6510.servicebased.catalog.persistence;

import edu.cs6510.servicebased.catalog.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

/** 商品表的数据访问接口 Spring Data JPA 负责实现查询 */

public interface CatalogItemRepository extends JpaRepository<CatalogItem, String> {
    // 功能：从数据库按 SKU 升序读取全部商品。
    List<CatalogItem> findAllByOrderBySkuAsc();
}
