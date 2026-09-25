package edu.cs6510.layered_server.contract;

import java.math.BigDecimal;
import java.time.Instant;

public record TransactionResponse(
        String transactionId, String stationId, String status,
        int itemCount, BigDecimal runningTotal, Instant startedAt
) { }
