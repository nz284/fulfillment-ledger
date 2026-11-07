package dev.fulfillment;

import java.time.Duration;
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
    private final Tasks tasks;
    private final Ledger ledger;
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper mapper;
    EventBus(Tasks tasks, Ledger ledger, KafkaTemplate<String,String> kafka, ObjectMapper mapper) {
        this.tasks=tasks; this.ledger=ledger; this.kafka=kafka; this.mapper=mapper;
    }

    @Scheduled(fixedDelayString="${app.poll-ms:200}")
    void publish() {
        for (int i=0; i<50; i++) {
            var next = tasks.claim("OUTBOX");
            if (next.isEmpty()) return;
            Lease item = next.get();
            try {
                Event event = new Event(item.id(),item.orderId(),item.payload());
                kafka.send(TOPIC,item.orderId().toString(),mapper.writeValueAsString(event)).get(6,TimeUnit.SECONDS);
                tasks.finish(item,true,null);
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                Audit.warn("outbox.publish.failed",item.orderId(),failure);
                tasks.finish(item,false,null);
                return;
            }
        }
    }

    @KafkaListener(topics=TOPIC,groupId="fulfillment-dispatch-v1")
    void dispatch(String data) { ledger.dispatch(mapper.readValue(data,Event.class)); }

    @KafkaListener(topics=TOPIC,groupId="${app.timeline-group:fulfillment-timeline-v1}")
    void project(String data) { ledger.project(mapper.readValue(data,Event.class)); }
}
