package dev.fulfillment;

import jakarta.persistence.EntityManager;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("worker")
class BacklogMetrics {
    final EntityManager em;
    final AtomicLong seconds=new AtomicLong();
    BacklogMetrics(EntityManager em, MeterRegistry metrics) {
        this.em=em;
        metrics.gauge("fulfillment.oldest.pending.seconds",seconds);
    }
    @Scheduled(fixedDelay=5000)
    void sample() {
        Number age=(Number)em.createNativeQuery("""
            SELECT COALESCE(EXTRACT(EPOCH FROM now()-MIN(created_at)),0)
            FROM fulfillment_order WHERE state <> 'CONFIRMED'
            """).getSingleResult();
        seconds.set(age.longValue());
    }
}
