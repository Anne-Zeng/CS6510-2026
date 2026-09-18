package edu.cs6510.monolith_server.transaction.api;

import jakarta.validation.constraints.NotBlank;

//ScanItemRequest.java describes what the client sends to your server:
// The ScanItemRequest record is a simple data transfer object (DTO) that represents the request payload for scanning an item during a checkout transaction.
// It contains a single field, sku, which is required to identify the item being scanned.
public record ScanItemRequest(
        @NotBlank(message = "sku is required")
        String sku
) {
}