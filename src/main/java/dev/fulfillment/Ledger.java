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

    OrderView accept(String tenant, String key, OrderRequest request) {
        UUID id = UUID.randomUUID();
        int created = em.createNativeQuery("""
            INSERT INTO fulfillment_order(id,tenant,request_key,sku,quantity,warehouse,state)
            VALUES (:id,:tenant,:key,:sku,:quantity,:warehouse,'ACCEPTED')
            ON CONFLICT (tenant,request_key) DO NOTHING
            """).setParameter("id",id).setParameter("tenant",tenant).setParameter("key",key)
            .setParameter("sku",request.sku()).setParameter("quantity",request.quantity())
            .setParameter("warehouse",request.warehouse()).executeUpdate();
        OrderEntity order = em.createQuery("from OrderEntity where tenant=:tenant and requestKey=:key",OrderEntity.class)
            .setParameter("tenant",tenant).setParameter("key",key).getSingleResult();
        if (!order.matches(request)) throw new ResponseStatusException(HttpStatus.CONFLICT,"Key reused with different order");
        if (created == 1) recordState(order);
        return order.view();
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

    void project(Event event) {
        em.createNativeQuery("""
            INSERT INTO timeline(event_id,order_id,state,sequence_no) VALUES (:id,:order,:state,:seq)
            ON CONFLICT DO NOTHING
            """).setParameter("id",event.id()).setParameter("order",event.orderId())
            .setParameter("state",event.state()).setParameter("seq",event.sequence()).executeUpdate();
    }

    void dispatch(Event event) {
        if (event.state().equals("ACCEPTED")) {
            OrderView order = get(event.orderId());
            // Each order has one durable dispatch task; redelivery leaves its progress intact.
            enqueue(order.id(),order.id(),"SHIP_" + order.warehouse(),"submit");
        }
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
