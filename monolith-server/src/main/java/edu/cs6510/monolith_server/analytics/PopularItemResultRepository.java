package edu.cs6510.monolith_server.analytics;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PopularItemResultRepository
        extends JpaRepository<PopularItemResult, Long> {

    List<PopularItemResult> findAllByWindowIdOrderByRankAsc(Long windowId);
}