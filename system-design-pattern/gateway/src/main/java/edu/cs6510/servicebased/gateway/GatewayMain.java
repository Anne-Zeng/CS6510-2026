package edu.cs6510.servicebased.gateway;

import edu.cs6510.events.KafkaBus;

/** Starts only after Kafka has assigned the gateway's reply subscription. */
public final class GatewayMain {
    private GatewayMain() {}

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromArgs(args);
        KafkaBus bus = new KafkaBus("gateway");
        GatewayServer gateway;
        try {
            gateway = new GatewayServer(config, bus);
        } catch (Exception e) {
            bus.close();
            throw e;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gateway.close();
            bus.close();
        }, "gateway-shutdown"));
        gateway.start();
        System.out.println("Gateway listening on port " + gateway.port() + "; upstream communication uses Kafka");
    }
}
