package edu.cs6510.monolith_server.transaction.api;

import jakarta.validation.constraints.NotBlank;

// The StartTransactionRequest record is a simple data transfer object (DTO) that represents the request payload for starting a new checkout transaction. 
// It contains a single field, stationId, which is required to identify the station where the transaction is being initiated. 
// The @NotBlank annotation ensures that the stationId field is not null or empty when the request is validated, providing a way to enforce input constraints and improve data integrity in the application.
public record StartTransactionRequest(
        @NotBlank(message = "stationId is required")
        String stationId
) {
}