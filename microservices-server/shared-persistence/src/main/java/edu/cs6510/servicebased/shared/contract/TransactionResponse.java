package edu.cs6510.servicebased.shared.contract;

import java.math.BigDecimal;
import java.time.Instant;

/** 功能：交易状态响应：标识、收银台、状态、累计件数和金额。 */

public record TransactionResponse(
        String transactionId, String stationId, String status,
        int itemCount, BigDecimal runningTotal, Instant startedAt
) { }
