package dev.fulfillment;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.persistence.EntityManager;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Service
@Transactional
class WarehouseBook {
    private final EntityManager em;
    private final tools.jackson.databind.ObjectMapper json;
    WarehouseBook(EntityManager em, tools.jackson.databind.ObjectMapper json) { this.em=em; this.json=json; }
    void acceptBatch(List<Shipment> shipments) {
        int count=em.createNativeQuery("""
            INSERT INTO shipment AS s(id,warehouse,sku,quantity)
            SELECT id,warehouse,sku,quantity FROM jsonb_to_recordset(cast(:items as jsonb))
                AS b(id uuid,warehouse text,sku text,quantity int) ORDER BY id
            ON CONFLICT(id) DO UPDATE SET sku=excluded.sku
            WHERE (s.sku,s.warehouse,s.quantity)=(excluded.sku,excluded.warehouse,excluded.quantity)
            """).setParameter("items",json.writeValueAsString(shipments)).executeUpdate();
        if (count!=shipments.size()) throw new ResponseStatusException(HttpStatus.CONFLICT);
    }
    void accept(Shipment s) {
        em.createNativeQuery("""
          INSERT INTO shipment(id,warehouse,sku,quantity) VALUES (:id,:warehouse,:sku,:qty)
          ON CONFLICT(id) DO NOTHING
          """).setParameter("id",s.id()).setParameter("warehouse",s.warehouse()).setParameter("sku",s.sku())
          .setParameter("qty",s.quantity()).executeUpdate();
        if (!s.equals(get(s.id()))) throw new ResponseStatusException(HttpStatus.CONFLICT);
    }
    Shipment get(UUID id) {
        List<?> rows=em.createNativeQuery("SELECT warehouse,sku,quantity FROM shipment WHERE id=:id")
            .setParameter("id",id).getResultList();
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        Object[] row=(Object[]) rows.getFirst();
        return new Shipment(id,(String)row[0],(String)row[1],(Integer)row[2]);
    }
}

@RestController
@Profile("warehouse")
class WarehouseApi {
    private final WarehouseBook book;
    private final Set<String> slow=ConcurrentHashMap.newKeySet();
    private final Set<String> unavailable=ConcurrentHashMap.newKeySet();
    WarehouseApi(WarehouseBook book) { this.book=book; }

    @PostMapping("/shipments")
    void submit(@RequestBody Shipment s) throws InterruptedException {
        checkAvailability(s);
        book.accept(s);
        Audit.info("shipment.submit.committed",s.id(),s.warehouse());
        // Commit first: the client may time out while the shipment already exists.
        if (slow.contains(s.warehouse())) Thread.sleep(3500);
    }
    @GetMapping("/shipments/{id}") Shipment lookup(@PathVariable UUID id) { return book.get(id); }
    @PostMapping("/shipments/batch")
    void batch(@RequestBody List<Shipment> items) throws InterruptedException {
        if (items.isEmpty() || items.size()>256) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        items.forEach(this::checkAvailability);
        book.acceptBatch(items);
        if (items.stream().anyMatch(item -> slow.contains(item.warehouse()))) Thread.sleep(3500);
    }
    @PutMapping("/demo/slow/{warehouse}") void slow(@PathVariable String warehouse) { slow.add(warehouse); }
    @DeleteMapping("/demo/slow/{warehouse}") void recover(@PathVariable String warehouse) { slow.remove(warehouse); }
    @PutMapping("/demo/submit-unavailable/{warehouse}") void reject(@PathVariable String warehouse) { unavailable.add(warehouse); }
    @DeleteMapping("/demo/submit-unavailable/{warehouse}") void resume(@PathVariable String warehouse) { unavailable.remove(warehouse); }

    private void checkAvailability(Shipment shipment) {
        if (unavailable.contains(shipment.warehouse())) {
            Audit.info("warehouse.submit.rejected",shipment.id(),shipment.warehouse());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}
