package edu.cs6510.servicebased.gateway;

import java.net.URI;

/** 功能：集中管理入口端口和四个业务服务地址，避免把部署地址写死在路由中。 */
public record GatewayConfig(int port, URI catalogUrl, URI checkoutUrl, URI analyticsUrl, URI inventoryUrl) {
    /** 功能：启动前检查端口和上游地址，及时报告配置错误；端口 0 供集成测试使用。 */
    public GatewayConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Gateway port must be between 0 and 65535");
        }
        validateBaseUrl(catalogUrl);
        validateBaseUrl(checkoutUrl);
        validateBaseUrl(analyticsUrl);
        validateBaseUrl(inventoryUrl);
    }

    /** 功能：读取明确的 --name=value 参数，缺省值对应本机四个独立服务。 */
    public static GatewayConfig fromArgs(String[] args) {
        int port = 8080;
        URI catalog = URI.create("http://127.0.0.1:8081");
        URI checkout = URI.create("http://127.0.0.1:8082");
        URI analytics = URI.create("http://127.0.0.1:8083");
        URI inventory = URI.create("http://127.0.0.1:8084");
        for (String arg : args) {
            int separator = arg.indexOf('=');
            if (separator < 0) {
                throw new IllegalArgumentException("Expected --name=value, received: " + arg);
            }
            String value = arg.substring(separator + 1);
            switch (arg.substring(0, separator)) {
                case "--port" -> port = Integer.parseInt(value);
                case "--catalog-url" -> catalog = URI.create(value);
                case "--checkout-url" -> checkout = URI.create(value);
                case "--analytics-url" -> analytics = URI.create(value);
                case "--inventory-url" -> inventory = URI.create(value);
                default -> throw new IllegalArgumentException("Unknown gateway argument: " + arg);
            }
        }
        return new GatewayConfig(port, catalog, checkout, analytics, inventory);
    }

    /** 功能：要求上游为 HTTP(S) 根地址，避免路径、认证信息或查询参数意外改变转发目标。 */
    private static void validateBaseUrl(URI url) {
        if (url == null || !("http".equalsIgnoreCase(url.getScheme())
                || "https".equalsIgnoreCase(url.getScheme()))
                || url.getHost() == null || url.getUserInfo() != null
                || url.getQuery() != null || url.getFragment() != null
                || !(url.getPath().isEmpty() || "/".equals(url.getPath()))) {
            throw new IllegalArgumentException("Upstream must be an HTTP(S) root URL: " + url);
        }
    }
}
