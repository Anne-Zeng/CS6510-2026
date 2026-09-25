package edu.cs6510.layered_server.contract;

public record ApiError(
        String error,
        String message
) {
}
