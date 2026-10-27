package edu.cs6510.events;

/** id is stable during Kafka redelivery; key identifies the transaction or saga. */
public record Request(String id, String operation, String key, String body, String replyTo) {}
