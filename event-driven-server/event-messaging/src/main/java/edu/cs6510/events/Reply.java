package edu.cs6510.events;

/** A committed business outcome; HTTP status is preserved at the external gateway boundary. */
public record Reply(int status, String body) {}
