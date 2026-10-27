package edu.cs6510.servicebased.gateway;

/** The public HTTP listener is configured independently of Kafka deployment settings. */
public record GatewayConfig(int port) {
    public GatewayConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Gateway port must be between 0 and 65535");
        }
    }

    public static GatewayConfig fromArgs(String[] args) {
        int port = 8080;
        for (String arg : args) {
            if (!arg.startsWith("--port=")) {
                throw new IllegalArgumentException("Unknown gateway argument: " + arg);
            }
            port = Integer.parseInt(arg.substring("--port=".length()));
        }
        return new GatewayConfig(port);
    }
}
