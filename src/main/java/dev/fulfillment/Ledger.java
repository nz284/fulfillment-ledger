package dev.fulfillment;

import jakarta.persistence.*;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@Transactional
class Ledger {
    @PersistenceContext EntityManager em;

    private final tools.jackson.databind.ObjectMapper json;
    Ledger(tools.jackson.databind.ObjectMapper json) { this.json=json; }

    record Input(String tenant,String key,String warehouse,String sku,int quantity) {}
    Map<OrderKey,OrderView> acceptBatch(List<Admission> batch) {
        var inputs=new LinkedHashMap<OrderKey,Input>();
        for (Admission item:batch) {
            var r=item.request();
            inputs.putIfAbsent(item.key(),new Input(item.key().tenant(),item.key().requestKey(),
                r.warehouse(),r.sku(),r.quantity()));
        }
        String rows=json.writeValueAsString(inputs.values());
        List<Object[]> inserted=em.createNativeQuery("""
            WITH accepted AS (
                INSERT INTO fulfillment_order(id,tenant,request_key,sku,quantity,warehouse,state)
                SELECT ledger_uuid_v7(),tenant,key,sku,quantity,warehouse,'ACCEPTED'
                FROM jsonb_to_recordset(cast(:rows as jsonb))
                    AS b(tenant text,key text,sku text,quantity integer,warehouse text)
                ORDER BY tenant,key ON CONFLICT (tenant,request_key) DO NOTHING
                RETURNING id,tenant,request_key,warehouse,sku,quantity,state
            ), audit AS (
                INSERT INTO history(id,order_id,state) SELECT ledger_uuid_v7(),id,state FROM accepted
                RETURNING id,order_id,state
            ), queued AS (
                INSERT INTO work_item(id,order_id,kind,payload) SELECT id,order_id,'OUTBOX',state FROM audit
            )
            SELECT tenant,request_key,id,warehouse,sku,quantity,state FROM accepted
            """,Object[].class).setParameter("rows",rows).getResultList();
        var saved=new HashMap<OrderKey,OrderView>();
        collect(inserted,saved);
        if (saved.size()<inputs.size()) {
            // A fresh READ COMMITTED statement sees competing transactions that won the key.
            saved.keySet().forEach(inputs::remove);
            List<Object[]> existing=em.createNativeQuery("""
                SELECT o.tenant,o.request_key,o.id,o.warehouse,o.sku,o.quantity,o.state
                FROM jsonb_to_recordset(cast(:rows as jsonb)) AS b(tenant text,key text)
                JOIN fulfillment_order o ON o.tenant=b.tenant AND o.request_key=b.key
                """,Object[].class).setParameter("rows",json.writeValueAsString(inputs.values())).getResultList();
            collect(existing,saved);
        }
        return saved;
    }

    private void collect(List<Object[]> rows,Map<OrderKey,OrderView> saved) {
        for (var row:rows) saved.put(new OrderKey((String)row[0],(String)row[1]),
            new OrderView((UUID)row[2],(String)row[3],(String)row[4],(Integer)row[5],(String)row[6]));
    }

    OrderView get(UUID id) {
        OrderEntity order = em.find(OrderEntity.class,id);
        if (order == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        return order.view();
    }

    OrderView forTenant(UUID id, String tenant) {
        OrderEntity order = em.find(OrderEntity.class,id);
        if (order == null || !order.tenant.equals(tenant)) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        return order.view();
    }

    void recordState(OrderEntity order) {
        UUID event = UUID.randomUUID();
        em.createNativeQuery("INSERT INTO history(id,order_id,state) VALUES (:id,:order,:state)")
            .setParameter("id",event).setParameter("order",order.id).setParameter("state",order.state).executeUpdate();
        enqueue(event,order.id,"OUTBOX",order.state);
    }

    void enqueue(UUID event, UUID order, String kind, String payload) {
        em.createNativeQuery("""
            INSERT INTO work_item(id,order_id,kind,payload) VALUES (:id,:order,:kind,:payload)
            ON CONFLICT DO NOTHING
            """).setParameter("id",event).setParameter("order",order).setParameter("kind",kind)
            .setParameter("payload",payload).executeUpdate();
    }

    List<?> timeline(UUID order) {
        return em.createNativeQuery("SELECT state FROM timeline WHERE order_id=:order ORDER BY sequence_no")
            .setParameter("order",order).getResultList();
    }

    String directory(String id) {
        return (String) em.createNativeQuery("SELECT name FROM warehouse_directory WHERE id=:id")
            .setParameter("id",id).getResultStream().findFirst().orElseThrow();
    }
}
