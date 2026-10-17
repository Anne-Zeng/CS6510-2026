package edu.cs6510.servicebased.shared.persistence;

import edu.cs6510.servicebased.shared.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.util.Optional;

/** 功能：交易主表访问；写操作通过行锁串行化同一购物篮的修改。 */

public interface CheckoutTransactionRepository
        extends JpaRepository<CheckoutTransaction, String> {
    // 功能：扫描/完成前锁住交易行；同一购物篮的并发修改按数据库锁顺序执行。
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from CheckoutTransaction t where t.transactionId = :id")
    Optional<CheckoutTransaction> findForUpdate(@Param("id") String id);
}
