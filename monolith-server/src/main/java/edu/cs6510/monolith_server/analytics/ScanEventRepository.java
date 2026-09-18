package edu.cs6510.monolith_server.analytics;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ScanEventRepository extends JpaRepository<ScanEvent, Long> {

    List<ScanEvent> findTop1000ByOrderBySequenceNumberDesc();
}