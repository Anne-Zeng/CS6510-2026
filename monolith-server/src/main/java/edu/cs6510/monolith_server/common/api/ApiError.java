package edu.cs6510.monolith_server.common.api;

public record ApiError(
        String error,
        String message
) {
}