package edu.cs6510.servicebased.analytics.persistence;

import edu.cs6510.servicebased.analytics.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/** 功能：查询已保存窗口及最大窗口终点，供幂等保存与恢复使用。 */

public interface AnalyticsWindowRepository
        extends JpaRepository<AnalyticsWindow, Long> {

    // 功能：检查窗口是否已经保存，避免重试产生重复排名。
    boolean existsByWindowEnd(long windowEnd);

    // 功能：查询最后成功保存的窗口，作为恢复检查点。
    Optional<AnalyticsWindow> findTopByOrderByWindowEndDesc();
}
