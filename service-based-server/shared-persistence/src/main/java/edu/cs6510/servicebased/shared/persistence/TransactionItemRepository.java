package edu.cs6510.servicebased.shared.persistence;

import edu.cs6510.servicebased.shared.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/** 功能：购物篮访问：按交易和 SKU 查单行，或读取交易全部明细。 */

public interface TransactionItemRepository
        extends JpaRepository<TransactionItem, Long> {

    // 功能：定位购物篮中某个 SKU，支持合并重复扫描。
    Optional<TransactionItem> findByTransactionTransactionIdAndSku(
            String transactionId,
            String sku
    );

    // 功能：读取交易所有商品明细，用于结账、收据和库存扣减。
    List<TransactionItem> findAllByTransactionTransactionId(String transactionId);
}
