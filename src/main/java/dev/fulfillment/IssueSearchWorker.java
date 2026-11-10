package dev.fulfillment;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
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
@Profile("search")
class SearchKafkaConfig {
    @Bean NewTopic issueChangesTopic() { return TopicBuilder.name(IssueSearchWorker.TOPIC).partitions(3).build(); }
    @Bean DefaultErrorHandler searchErrors() {
        return new DefaultErrorHandler(new FixedBackOff(2000,FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}

@Component
@Profile("search")
class IssueSearchWorker {
    static final String TOPIC="reconciliation.issue-changes.v1";
    private final IssueSearchStore store;
    private final IssueSearchProjection projection;
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper json;
    IssueSearchWorker(IssueSearchStore store,IssueSearchProjection projection,KafkaTemplate<String,String> kafka,ObjectMapper json) {
        this.store=store; this.projection=projection; this.kafka=kafka; this.json=json;
    }

    @Scheduled(fixedDelayString="${app.search.publish-ms:250}")
    void publish() {
        var batch=store.pending();
        if(batch.isEmpty()) return;
        try {
            var sends=batch.stream().map(change->kafka.send(TOPIC,change.issueId().toString(),json.writeValueAsString(change)))
                .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(sends).get(8,TimeUnit.SECONDS);
            store.acknowledged(batch);
        } catch(Exception e) {
            if(e instanceof InterruptedException) Thread.currentThread().interrupt();
            LoggerFactory.getLogger(getClass()).warn("Search outbox publish will retry",e);
        }
    }

    @KafkaListener(topics=TOPIC,groupId="reconciliation-issue-search-v1")
    void consume(List<ConsumerRecord<byte[],byte[]>> records) {
        projection.changes(records.stream().map(r->json.readValue(r.value(),IssueChange.class)).toList());
    }

    @Scheduled(fixedDelayString="${app.search.build-ms:1000}")
    void rebuild() { projection.advanceBuild(); }
}
