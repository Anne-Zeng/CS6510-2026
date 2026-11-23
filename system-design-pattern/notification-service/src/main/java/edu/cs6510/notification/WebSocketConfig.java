package edu.cs6510.notification;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
// WebSocketConfig.java 配置 WebSocket 处理器，将 /notifications 路径映射到 SubscriberHandler
// 注册共同的 WebSocket 地址，允许客户端通过 WebSocket 连接到通知服务，接收实时通知消息， 
// 所有订阅者访问同一个 URL，每次连接都会创建独立的 WebSocket session
// 每个订阅者创建自己的连接SubscriberMain.java
// Each subscriber opens a separate WebSocket connection to notification service, 
// allowing for individualized message delivery and acknowledgment handling.
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    private final SubscriberHandler handler;
    public WebSocketConfig(SubscriberHandler handler) { this.handler = handler; }
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/notifications");
    }
}
