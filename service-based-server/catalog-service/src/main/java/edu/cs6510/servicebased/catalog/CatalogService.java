package edu.cs6510.servicebased.catalog;

import edu.cs6510.servicebased.shared.contract.CatalogItemResponse;
import edu.cs6510.servicebased.shared.contract.CatalogResponse;
import edu.cs6510.servicebased.shared.persistence.CatalogCache;
import org.springframework.stereotype.Service;

/** 功能：将共享数据库的只读目录快照转换为客户端 API 响应。 */
@Service
public class CatalogService {
    private final CatalogCache catalog;

    /** 功能：复用数据库访问层缓存，减少重复查询固定的商品目录。 */
    public CatalogService(CatalogCache catalog) {
        this.catalog = catalog;
    }

    /** 功能：按 SKU 稳定排序并只暴露目录字段，避免向客户端返回数据库实体。 */
    public CatalogResponse getCatalog() {
        return new CatalogResponse(catalog.findAllOrderedBySku().stream()
                .map(item -> new CatalogItemResponse(item.getSku(), item.getName(), item.getPrice()))
                .toList());
    }
}
