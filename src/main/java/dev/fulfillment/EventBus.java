package dev.fulfillment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

@Configuration
@Profile("worker")
class KafkaConfig {
    @Bean NewTopic events() { return TopicBuilder.name(EventBus.TOPIC).partitions(6).build(); }

}

@Component
@Profile("worker")
class EventBus {
    static final String TOPIC = "fulfillment.events.v1";
    private static final org.slf4j.Logger timings =
        org.slf4j.LoggerFactory.getLogger("fulfillment.dispatch.timing");
    private final BatchStore batches;
    private final EventConsumer consumer;
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper mapper;
    EventBus(BatchStore batches, KafkaTemplate<String,String> kafka, ObjectMapper mapper, EventConsumer consumer) {
        this.consumer=consumer; this.batches=batches; this.kafka=kafka; this.mapper=mapper;
    }

    @Scheduled(fixedDelayString="${app.poll-ms:5}")
    void publish() {
        var claims = batches.claim("OUTBOX",256);
        if (claims.isEmpty()) return;
        try {
            var sends = claims.stream().map(item -> {
                Event event = new Event(item.id(),item.orderId(),item.payload());
                var send=kafka.send(TOPIC,item.orderId().toString(),mapper.writeValueAsString(event));
                if (timings.isDebugEnabled() && event.state().equals("ACCEPTED")) {
                    return send.thenAccept(result -> timings.atDebug().addKeyValue("event","dispatch.acked")
                        .addKeyValue("fulfillment_id",event.orderId())
                        .addKeyValue("partition",result.getRecordMetadata().partition()).log("dispatch.acked"));
                }
                return send;
            }).toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(sends).get(6,TimeUnit.SECONDS);
            batches.published(claims);
        } catch (Exception failure) {
            // Unacknowledged batches become eligible again at lease expiry.
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            Audit.warn("outbox.publish.failed",claims.getFirst().orderId(),failure);
        }
    }

    @KafkaListener(topics=TOPIC, groupId=DeadLetters.DISPATCH_GROUP)
    void dispatch(List<ConsumerRecord<byte[],byte[]>> records) { consumer.dispatch(records); }

    @KafkaListener(topics=TOPIC, groupId="${app.timeline-group:fulfillment-timeline-v1}")
    void project(List<ConsumerRecord<byte[],byte[]>> records) { consumer.project(records); }
}
