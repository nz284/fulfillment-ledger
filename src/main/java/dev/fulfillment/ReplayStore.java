package dev.fulfillment;

import jakarta.persistence.EntityManager;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class ReplayStore {
    private final EntityManager em;
    private final EventConsumer decoder;
    private final BatchStore batches;
    private final String timelineGroup;

    ReplayStore(EntityManager em, EventConsumer decoder, BatchStore batches,
            @Value("${app.timeline-group:fulfillment-timeline-v1}") String timelineGroup) {
        this.em = em;
        this.decoder = decoder;
        this.batches = batches;
        this.timelineGroup = timelineGroup;
    }

    private static byte[] header(ConsumerRecord<?,?> record, String name) {
        var header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) throw new IllegalArgumentException("Missing header " + name);
        return header.value();
    }

    @Transactional
    boolean replay(ConsumerRecord<byte[],byte[]> record, byte[] replacement) {
        String source = new String(header(record,KafkaHeaders.DLT_ORIGINAL_TOPIC),StandardCharsets.UTF_8);
        String group = new String(header(record,KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),StandardCharsets.UTF_8);
        int partition = ByteBuffer.wrap(header(record,KafkaHeaders.DLT_ORIGINAL_PARTITION)).getInt();
        long offset = ByteBuffer.wrap(header(record,KafkaHeaders.DLT_ORIGINAL_OFFSET)).getLong();
        boolean dispatch = record.topic().equals(DeadLetters.DISPATCH) && group.equals(DeadLetters.DISPATCH_GROUP);
        boolean timeline = record.topic().equals(DeadLetters.TIMELINE) && group.equals(timelineGroup);
        if (!(dispatch || timeline) || !source.equals(EventBus.TOPIC) || partition != record.partition() || offset < 0)
            throw new IllegalArgumentException("Unexpected DLT source or consumer group");

        Event event = decoder.decode(replacement == null ? record.value() : replacement);
        if (record.key() != null && !event.orderId().toString().equals(new String(record.key(),StandardCharsets.UTF_8)))
            throw new IllegalArgumentException("Corrected orderId differs from the original Kafka key");
        int inserted = em.createNativeQuery("""
            INSERT INTO dead_letter_replay(consumer_group,source_topic,source_partition,source_offset,event_id,order_id,state)
            VALUES (:grp,:topic,:part,:off,:id,:ord,:state) ON CONFLICT DO NOTHING
            """).setParameter("grp",group).setParameter("topic",source).setParameter("part",partition)
            .setParameter("off",offset).setParameter("id",event.id()).setParameter("ord",event.orderId())
            .setParameter("state",event.state()).executeUpdate();
        if (inserted == 0) {
            long identical = (Long) em.createNativeQuery("""
                SELECT count(*) FROM dead_letter_replay WHERE consumer_group=:grp AND source_topic=:topic
                AND source_partition=:part AND source_offset=:off AND event_id=:id AND order_id=:ord AND state=:state
                """,Long.class).setParameter("grp",group).setParameter("topic",source).setParameter("part",partition)
                .setParameter("off",offset).setParameter("id",event.id()).setParameter("ord",event.orderId())
                .setParameter("state",event.state()).getSingleResult();
            if (identical != 1) throw new IllegalStateException("Source coordinate already replayed with another payload");
            return false;
        }
        if (dispatch) batches.dispatch(List.of(event));
        else batches.project(List.of(event));
        return true;
    }
}
