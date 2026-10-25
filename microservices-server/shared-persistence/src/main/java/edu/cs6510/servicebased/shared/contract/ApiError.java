package edu.cs6510.servicebased.shared.contract;

/** 功能：统一错误响应：错误代码和可读消息，保持各服务的 HTTP 合同一致。 */

public record ApiError(
        String error,
        String message
) {
}
