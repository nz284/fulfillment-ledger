package dev.fulfillment;

import jakarta.persistence.*;
import java.time.Duration;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
class Tasks {
    @PersistenceContext EntityManager em;
    private final Ledger ledger;
    Tasks(Ledger ledger) { this.ledger = ledger; }

    private void delay(java.util.UUID id, long seconds) {
        em.createNativeQuery("UPDATE work_item SET available_at=now()+(:seconds * interval '1 second') WHERE id=:id")
            .setParameter("seconds",seconds).setParameter("id",id).executeUpdate();
    }

    void finish(Lease lease, boolean completed, String state) {
        WorkItem row = em.find(WorkItem.class,lease.id(),LockModeType.PESSIMISTIC_WRITE);
        if (row.done || row.token != lease.token()) return;
        if (state != null) {
            OrderEntity order = em.find(OrderEntity.class,lease.orderId(),LockModeType.PESSIMISTIC_WRITE);
            if (!order.state.equals(state) && !order.state.equals("CONFIRMED")) {
                order.state = state;
                ledger.recordState(order);
            }
        }
        row.done = completed;
        if (!completed) delay(row.id,Math.min(Duration.ofMinutes(2).toSeconds(),1L << Math.min(lease.attempts(),7)));
    }
}
