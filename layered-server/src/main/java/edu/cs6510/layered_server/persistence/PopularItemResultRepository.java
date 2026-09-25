package edu.cs6510.layered_server.persistence;

import edu.cs6510.layered_server.persistence.entity.*;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PopularItemResultRepository
        extends JpaRepository<PopularItemResult, Long> {

    List<PopularItemResult> findAllByWindowIdOrderByRankAsc(Long windowId);
}
