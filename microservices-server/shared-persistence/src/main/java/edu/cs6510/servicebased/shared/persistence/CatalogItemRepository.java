package edu.cs6510.servicebased.shared.persistence;

import edu.cs6510.servicebased.shared.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

/** 功能：集中定义商品表查询，使多个进程复用相同的数据访问合同。 */

public interface CatalogItemRepository extends JpaRepository<CatalogItem, String> {
    // 功能：从数据库按 SKU 升序读取全部商品。
    List<CatalogItem> findAllByOrderBySkuAsc();
}
