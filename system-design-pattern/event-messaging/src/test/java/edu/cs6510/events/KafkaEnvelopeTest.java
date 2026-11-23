package edu.cs6510.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaEnvelopeTest {
    private ConsumerRecord<String,String> consumed(ProducerRecord<String,String> produced) {
        var record=new ConsumerRecord<>(produced.topic(),0,12,produced.key(),produced.value());
        produced.headers().forEach(header->record.headers().add(header));
        return record;
    }

    @Test void headersRetainIdentitiesAndExactOpaqueUnicodeJsonWithoutDoubleEncoding() {
        String body="{\"sku\":\"咖啡\\\"A\",\"price\":0.10,\"note\":\"line\\nnext\"}";
        var request=new Request("request-1","SCAN_ITEM","basket-2",body,"test.gateway.replies");
        var message=KafkaEnvelope.request("test.checkout.requests",request,true);
        assertEquals(body,message.value());
        assertEquals(request,KafkaEnvelope.request(consumed(message)));
        var reply=new KafkaBus.CorrelatedReply(request.id(),new Reply(409,body));
        var response=KafkaEnvelope.reply(request.replyTo(),reply,true);
        assertEquals(body,response.value());
        assertEquals(reply,KafkaEnvelope.reply(consumed(response)));
    }

    @Test void retainedLegacyRequestsAndRepliesStillDecodeWithTheirOriginalIds() {
        var request=new Request("old-command","START_TRANSACTION","station","{}","test.gateway.replies");
        var command=consumed(KafkaEnvelope.request("test.checkout.requests",request,false));
        assertFalse(KafkaEnvelope.hasHeaders(command));
        assertEquals(request,KafkaEnvelope.request(command));
        var reply=new KafkaBus.CorrelatedReply(request.id(),new Reply(201,"{\"runningTotal\":0.00}"));
        var response=consumed(KafkaEnvelope.reply(request.replyTo(),reply,KafkaEnvelope.hasHeaders(command)));
        assertFalse(KafkaEnvelope.hasHeaders(response),"An old caller must receive its original JSON encoding");
        assertEquals(reply,KafkaEnvelope.reply(response));
    }

    @Test void missingDuplicateOrUnknownMetadataCannotBeTreatedAsAValidCommand() {
        var request=new Request("request-1","SCAN_ITEM",null,"{}","test.gateway.replies");
        var record=consumed(KafkaEnvelope.request("commands",request,true));
        assertEquals(request,KafkaEnvelope.request(record));
        record.headers().remove("sc.id");
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.request(record));
        record.headers().add("sc.id",new byte[]{1}).add("sc.id",new byte[]{2});
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.request(record));
        record.headers().remove("sc.id").add("sc.id",new byte[]{1});
        record.headers().remove("sc.format").add("sc.format","request-v99".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.request(record));
    }

    @Test void wrongMessageKindOrInvalidReplyStatusCannotBecomeASuccessfulReply() {
        var request=new Request("request-1","SCAN_ITEM","basket","{}","test.gateway.replies");
        var command=consumed(KafkaEnvelope.request("commands",request,true));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.reply(command));
        var reply=consumed(KafkaEnvelope.reply("replies",new KafkaBus.CorrelatedReply("request-1",new Reply(200,"{}")),true));
        reply.headers().remove("sc.status").add("sc.status","999".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.reply(reply));
    }
}
