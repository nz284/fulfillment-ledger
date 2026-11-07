package dev.fulfillment;

import jakarta.persistence.EntityManager;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** Each method commits one bounded batch. Lease tokens fence slow or restarted workers. */
@Service
@Transactional
class BatchStore {
    private final EntityManager em;
    private final ObjectMapper json;

    BatchStore(EntityManager em, ObjectMapper json) {
        this.em = em;
        this.json = json;
    }

    List<Lease> claim(String kind, int limit) {
        List<Object[]> rows = em.createNativeQuery("""
            WITH ready AS (
                SELECT id FROM work_item WHERE kind=:kind AND NOT done AND available_at<=now()
                ORDER BY available_at,id LIMIT :limit FOR UPDATE SKIP LOCKED
            )
            UPDATE work_item w SET available_at=now()+interval '30 seconds',
                attempts=attempts+1, token=token+1 FROM ready WHERE w.id=ready.id
            RETURNING w.id,w.order_id,w.payload,w.token,w.attempts
            """, Object[].class).setParameter("kind",kind).setParameter("limit",limit).getResultList();
        return rows.stream().map(row -> new Lease((java.util.UUID)row[0],(java.util.UUID)row[1],
            (String)row[2],(Long)row[3],(Integer)row[4])).toList();
    }

    void published(List<Lease> sent) {
        em.createNativeQuery("""
            DELETE FROM work_item w USING jsonb_to_recordset(cast(:batch as jsonb))
            AS b(id uuid,token bigint) WHERE w.id=b.id AND w.token=b.token AND w.kind='OUTBOX' AND NOT w.done
            """).setParameter("batch",json.writeValueAsString(sent)).executeUpdate();
    }

    void dispatch(List<Event> events) {
        em.createNativeQuery("""
            INSERT INTO work_item(id,order_id,kind,payload)
            SELECT o.id,o.id,'SHIP_'||o.warehouse,'submit'
            FROM jsonb_to_recordset(cast(:batch as jsonb)) AS b("orderId" uuid,state text)
            JOIN fulfillment_order o ON o.id=b."orderId" WHERE b.state='ACCEPTED'
            ON CONFLICT DO NOTHING
            """).setParameter("batch",json.writeValueAsString(events)).executeUpdate();
    }

    void project(List<Event> events) {
        em.createNativeQuery("""
            INSERT INTO timeline(event_id,order_id,state,sequence_no)
            SELECT id,"orderId",state,CASE state WHEN 'ACCEPTED' THEN 1 WHEN 'UNCONFIRMED' THEN 2
                WHEN 'CONFIRMED' THEN 3 END
            FROM jsonb_to_recordset(cast(:batch as jsonb)) AS b(id uuid,"orderId" uuid,state text)
            ON CONFLICT DO NOTHING
            """).setParameter("batch",json.writeValueAsString(events)).executeUpdate();
    }

    List<Shipment> shipments(List<Lease> claims) {
        List<Object[]> rows = em.createNativeQuery("""
            SELECT o.id,o.warehouse,o.sku,o.quantity FROM fulfillment_order o
            JOIN jsonb_to_recordset(cast(:batch as jsonb)) AS b("orderId" uuid) ON o.id=b."orderId"
            """, Object[].class).setParameter("batch",json.writeValueAsString(claims)).getResultList();
        return rows.stream().map(row -> new Shipment((java.util.UUID)row[0],(String)row[1],
            (String)row[2],(Integer)row[3])).toList();
    }

    List<java.util.UUID> confirmed(List<Lease> claims) {
        return em.createNativeQuery("""
            WITH completed AS (
                UPDATE work_item w SET done=true FROM jsonb_to_recordset(cast(:batch as jsonb))
                AS b(id uuid,token bigint) WHERE w.id=b.id AND w.token=b.token AND NOT w.done
                RETURNING w.order_id
            ), changed AS (
                UPDATE fulfillment_order o SET state='CONFIRMED',revision=revision+1
                FROM completed c WHERE o.id=c.order_id AND o.state<>'CONFIRMED' RETURNING o.id
            ), audit AS (
                INSERT INTO history(id,order_id,state) SELECT ledger_uuid_v7(),id,'CONFIRMED' FROM changed
                RETURNING id,order_id,state
            ), queued AS (
                INSERT INTO work_item(id,order_id,kind,payload) SELECT id,order_id,'OUTBOX',state FROM audit
            )
            SELECT order_id FROM audit
            """,java.util.UUID.class).setParameter("batch",json.writeValueAsString(claims)).getResultList();
    }
}
