package edu.cs6510.monolith_server.analytics;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AnalyticsWindowRepository
        extends JpaRepository<AnalyticsWindow, Long> {

    Optional<AnalyticsWindow> findTopByOrderByWindowEndDesc();
}