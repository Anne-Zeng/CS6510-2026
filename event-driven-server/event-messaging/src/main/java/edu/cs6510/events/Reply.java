package edu.cs6510.events;
//表示最终业务响应，包括状态和响应内容。Reply自己不会发消息，KafkaBus会将它与原请求的 id 一起发送到 replyTo，让请求方知道
/** A committed business outcome; HTTP status is preserved at the external gateway boundary. */
public record Reply(int status, String body) {}
