package edu.cs6510.pipeline_server.persistence;

import edu.cs6510.pipeline_server.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AnalyticsWindowRepository
        extends JpaRepository<AnalyticsWindow, Long> {

    boolean existsByWindowEnd(long windowEnd);

    Optional<AnalyticsWindow> findTopByOrderByWindowEndDesc();
}
