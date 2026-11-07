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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.ObjectMapper;

@Configuration
@Profile("worker")
class KafkaConfig {
    @Bean NewTopic events() { return TopicBuilder.name(EventBus.TOPIC).partitions(6).build(); }
    @Bean DefaultErrorHandler errors() {
        return new DefaultErrorHandler(new FixedBackOff(Duration.ofSeconds(2).toMillis(),FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}

@Component
@Profile("worker")
class EventBus {
    static final String TOPIC = "fulfillment.events.v1";
    private final BatchStore batches;
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper mapper;
    EventBus(BatchStore batches, KafkaTemplate<String,String> kafka, ObjectMapper mapper) {
        this.batches=batches; this.kafka=kafka; this.mapper=mapper;
    }

    @Scheduled(fixedDelayString="${app.poll-ms:5}")
    void publish() {
        var claims = batches.claim("OUTBOX",256);
        if (claims.isEmpty()) return;
        try {
            var sends = claims.stream().map(item -> {
                Event event = new Event(item.id(),item.orderId(),item.payload());
                return kafka.send(TOPIC,item.orderId().toString(),mapper.writeValueAsString(event));
            }).toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(sends).get(6,TimeUnit.SECONDS);
            batches.published(claims);
        } catch (Exception failure) {
            // Unacknowledged batches become eligible again at lease expiry.
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            Audit.warn("outbox.publish.failed",claims.getFirst().orderId(),failure);
        }
    }

    @KafkaListener(topics=TOPIC,groupId="fulfillment-dispatch-v1")
    void dispatch(List<String> data) { batches.dispatch(decode(data)); }

    @KafkaListener(topics=TOPIC,groupId="${app.timeline-group:fulfillment-timeline-v1}")
    void project(List<String> data) { batches.project(decode(data)); }

    private List<Event> decode(List<String> records) {
        return records.stream().map(data -> mapper.readValue(data,Event.class)).toList();
    }
}
