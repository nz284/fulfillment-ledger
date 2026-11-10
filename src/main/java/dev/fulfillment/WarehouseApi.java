package dev.fulfillment;

import java.util.*;
import java.time.Instant;
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

    List<ManifestLine> manifest(Instant from, Instant to, Instant afterTime, UUID afterId, int limit) {
        if (!from.isBefore(to) || limit<1 || limit>500 || (afterTime==null)!=(afterId==null))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid window, cursor or limit");
        String cursor=afterId==null ? "" : " AND (recorded_at,id)>(:time,:id)";
        var query=em.createNativeQuery("SELECT recorded_at,id,warehouse,sku,quantity FROM shipment " +
            "WHERE recorded_at>=:lo AND recorded_at<:hi"+cursor+" ORDER BY recorded_at,id",Object[].class)
            .setParameter("lo",from).setParameter("hi",to).setMaxResults(limit);
        if (afterId!=null) query.setParameter("time",afterTime).setParameter("id",afterId);
        List<Object[]> rows=query.getResultList();
        return rows.stream().map(r -> new ManifestLine(ScanKey.instant(r[0]),
            new Shipment((UUID)r[1],(String)r[2],(String)r[3],(Integer)r[4]))).toList();
    }

    List<Shipment> lookupMany(List<UUID> ids) {
        if (ids.isEmpty() || ids.size()>500 || ids.stream().anyMatch(Objects::isNull))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Supply 1 to 500 shipment IDs");
        List<Object[]> rows=em.createNativeQuery("SELECT id,warehouse,sku,quantity FROM shipment WHERE id IN (:ids)",Object[].class)
            .setParameter("ids",ids).getResultList();
        return rows.stream().map(r -> new Shipment((UUID)r[0],(String)r[1],(String)r[2],(Integer)r[3])).toList();
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
    @GetMapping("/shipments/manifest")
    List<ManifestLine> manifest(@RequestParam Instant from,@RequestParam Instant to,
            @RequestParam(required=false) Instant afterTime,@RequestParam(required=false) UUID afterId,
            @RequestParam(defaultValue="200") int limit) {
        return book.manifest(from,to,afterTime,afterId,limit);
    }
    @PostMapping("/shipments/lookup")
    List<Shipment> lookupMany(@RequestBody List<UUID> ids) { return book.lookupMany(ids); }
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
