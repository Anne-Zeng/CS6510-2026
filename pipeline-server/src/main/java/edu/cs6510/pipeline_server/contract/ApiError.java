package edu.cs6510.pipeline_server.contract;

public record ApiError(
        String error,
        String message
) {
}
