package dev.fulfillment;

import java.time.Duration;
import java.util.Map;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Profile("worker")
class DeadLetters implements AutoCloseable {
    static final String DISPATCH_GROUP = "fulfillment-dispatch-v1";
    static final String DISPATCH = "fulfillment.dispatch.dlt";
    static final String TIMELINE = "fulfillment.timeline.dlt";
    private final KafkaTemplate<?,?> publisher;

    DeadLetters(KafkaTemplate<String,String> kafka) {
        publisher = new KafkaTemplate<>(kafka.getProducerFactory(), Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @Bean KafkaAdmin.NewTopics deadLetterTopics() {
        return new KafkaAdmin.NewTopics(TopicBuilder.name(DISPATCH).partitions(6).build(),
            TopicBuilder.name(TIMELINE).partitions(6).build());
    }

    @Bean DefaultErrorHandler errors(MeterRegistry metrics,
            @Value("${app.timeline-group:fulfillment-timeline-v1}") String timelineGroup) {
        var groups = Map.of(DISPATCH_GROUP, DISPATCH, timelineGroup, TIMELINE);
        groups.keySet().forEach(group -> {
            metrics.counter("fulfillment.dlt.published", "group", group);
            metrics.counter("fulfillment.dlt.publish.errors", "group", group);
        });
        var recoverer = new DeadLetterPublishingRecoverer(publisher, (record, failure) -> {
            for (Throwable t = failure; t != null; t = t.getCause()) {
                if (t instanceof ListenerExecutionFailedException listener && listener.getGroupId() != null) {
                    String topic = groups.get(listener.getGroupId());
                    if (topic != null) return new TopicPartition(topic, record.partition());
                }
            }
            throw new IllegalStateException("Unknown consumer group; retaining offset", failure);
        });
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setVerifyPartition(false); // Require the matching partition; never silently reroute.
        ConsumerAwareRecordRecoverer recover = (record, consumer, failure) -> {
            if (recordFailure(failure) == null)
                throw new IllegalStateException("Unattributed failure; retaining offset", failure);
            String group = consumer.groupMetadata().groupId();
            try {
                recoverer.accept(record, consumer, failure);
            } catch (RuntimeException sendFailure) {
                metrics.counter("fulfillment.dlt.publish.errors","group",group).increment();
                // Synchronous broker rejections can return immediately. Pace redelivery
                // while leaving the source offset untouched.
                java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
                throw sendFailure;
            }
            metrics.counter("fulfillment.dlt.published","group",group).increment();
        };
        var outage = new FixedBackOff(Duration.ofSeconds(2).toMillis(),FixedBackOff.UNLIMITED_ATTEMPTS);
        var handler = new DefaultErrorHandler(recover, outage);
        handler.setBackOffFunction((record, failure) -> {
            Throwable cause = recordFailure(failure);
            if (cause instanceof InvalidEvent) return new FixedBackOff(0, 0);
            if (cause instanceof MissingOrder) return new FixedBackOff(1000, 2);
            return outage;
        });
        return handler;
    }

    private static Throwable recordFailure(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause())
            if (t instanceof InvalidEvent || t instanceof MissingOrder) return t;
        return null;
    }

    @Override public void close() { publisher.destroy(); }
}
