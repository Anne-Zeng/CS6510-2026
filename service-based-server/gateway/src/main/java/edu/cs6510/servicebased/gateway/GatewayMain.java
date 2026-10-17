package edu.cs6510.servicebased.gateway;

/** 功能：启动独立网关 JAR，向原有负载客户端提供统一的 8080 入口。 */
public final class GatewayMain {
    /** 功能：工具入口类不需要实例。 */
    private GatewayMain() {
    }

    /** 功能：读取部署参数、注册进程退出清理，并开始接受 HTTP 请求。 */
    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromArgs(args);
        GatewayServer gateway = new GatewayServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(gateway::close, "gateway-shutdown"));
        gateway.start();
        System.out.println("Gateway listening on port " + gateway.port());
    }
}
