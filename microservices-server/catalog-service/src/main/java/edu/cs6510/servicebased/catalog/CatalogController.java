package edu.cs6510.servicebased.catalog;

import edu.cs6510.servicebased.catalog.contract.CatalogResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 功能：保留负载客户端使用的 GET /items 接口。 */
@RestController
@RequestMapping("/items")
public class CatalogController {
    private final CatalogService catalog;

    /** 功能：通过依赖注入获得本进程的目录查询业务组件。 */
    public CatalogController(CatalogService catalog) {
        this.catalog = catalog;
    }

    /** 功能：以原有 JSON 格式返回商品编号、名称和价格。 */
    @GetMapping
    public CatalogResponse getCatalog() {
        return catalog.getCatalog();
    }
}
