package edu.cs6510.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
//将业务JSON 放在消息 value 中，将请求 ID、操作名等元数据放在 Kafka headers 中，减少反复包装和解析 JSON 的开销
/** Keep business JSON opaque: correlation metadata belongs in Kafka headers.
 * Readers also accept the original JSON envelope so retained commands can replay.
 */
final class KafkaEnvelope {
    private static final String FORMAT="sc.format",ID="sc.id",OPERATION="sc.op",REPLY_TO="sc.reply",STATUS="sc.status";
    private KafkaEnvelope() {}

    static ProducerRecord<String,String> request(String topic,Request request,boolean headers) {
        if(!headers) return new ProducerRecord<>(topic,request.key(),Json.write(request));
        var metadata=new RecordHeaders();
        put(metadata,FORMAT,"request-v1");put(metadata,ID,request.id());
        put(metadata,OPERATION,request.operation());put(metadata,REPLY_TO,request.replyTo());
        return new ProducerRecord<>(topic,null,null,request.key(),request.body(),metadata);
    }

    static Request request(ConsumerRecord<String,String> record) {
        if(!hasHeaders(record)) return Json.read(record.value(),Request.class);
        requireFormat(record.headers(),"request-v1");
        return new Request(get(record.headers(),ID),get(record.headers(),OPERATION),record.key(),
                record.value(),get(record.headers(),REPLY_TO));
    }

    static ProducerRecord<String,String> reply(String topic,KafkaBus.CorrelatedReply response,boolean headers) {
        if(!headers) return new ProducerRecord<>(topic,response.id(),Json.write(response));
        var metadata=new RecordHeaders();
        put(metadata,FORMAT,"reply-v1");put(metadata,ID,response.id());
        put(metadata,STATUS,Integer.toString(response.reply().status()));
        return new ProducerRecord<>(topic,null,null,response.id(),response.reply().body(),metadata);
    }

    static KafkaBus.CorrelatedReply reply(ConsumerRecord<String,String> record) {
        if(!hasHeaders(record)) return Json.read(record.value(),KafkaBus.CorrelatedReply.class);
        requireFormat(record.headers(),"reply-v1");
        int status=Integer.parseInt(get(record.headers(),STATUS));
        if(status<100 || status>599) throw new IllegalArgumentException("Invalid reply status");
        return new KafkaBus.CorrelatedReply(get(record.headers(),ID),new Reply(status,record.value()));
    }

    static boolean hasHeaders(ConsumerRecord<String,String> record) {
        return record.headers().lastHeader(FORMAT)!=null;
    }

    private static void requireFormat(Headers headers,String expected) {
        if(!get(headers,FORMAT).equals(expected)) throw new IllegalArgumentException("Unsupported Kafka envelope format");
    }

    private static void put(Headers headers,String name,String value) {
        headers.add(name,Objects.requireNonNull(value,name).getBytes(StandardCharsets.UTF_8));
    }

    private static String get(Headers headers,String name) {
        byte[] value=null;
        for(var header:headers.headers(name)) {
            if(value!=null || header.value()==null) throw new IllegalArgumentException("Invalid Kafka header: "+name);
            value=header.value();
        }
        if(value==null) throw new IllegalArgumentException("Missing Kafka header: "+name);
        return new String(value,StandardCharsets.UTF_8);
    }
}
