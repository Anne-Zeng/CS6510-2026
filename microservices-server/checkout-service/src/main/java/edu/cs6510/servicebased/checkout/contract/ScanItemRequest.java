package edu.cs6510.servicebased.checkout.contract;

import jakarta.validation.constraints.NotBlank;

/** 功能：扫描请求；NotBlank 在进入业务方法前拒绝空 SKU。 */

public record ScanItemRequest(
        @NotBlank(message = "sku is required")
        String sku
) {
}
