package edu.cs6510.events;
//表示收到的请求，包括请求 ID、操作、key、消息体(业务参数的 JSON), Gateway 的响应 topic:处理完成后向哪个 topic 发送响应
/** id is stable during Kafka redelivery; key identifies the transaction or saga. */
public record Request(String id, String operation, String key, String body, String replyTo) {}
