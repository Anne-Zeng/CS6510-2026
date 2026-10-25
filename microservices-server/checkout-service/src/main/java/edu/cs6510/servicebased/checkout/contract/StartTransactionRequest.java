package edu.cs6510.servicebased.checkout.contract;

import jakarta.validation.constraints.NotBlank;

/** 功能：创建交易的请求；stationId 必填，用于标识收银台。 */

public record StartTransactionRequest(
        @NotBlank(message = "stationId is required")
        String stationId
) {
}
