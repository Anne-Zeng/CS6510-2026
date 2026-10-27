package edu.cs6510.events;

public class BusException extends RuntimeException {
    public BusException(String message) { super(message); }
    public BusException(String message, Throwable cause) { super(message, cause); }
}
