package edu.cs6510.servicebased.analytics.persistence;

import edu.cs6510.servicebased.analytics.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/** 功能：读取指定分析窗口的排名行，按名次升序返回。 */

public interface PopularItemResultRepository
        extends JpaRepository<PopularItemResult, Long> {

    // 功能：读取同一窗口的全部排名并按 rank 排序。
    List<PopularItemResult> findAllByWindowIdOrderByRankAsc(Long windowId);
}
