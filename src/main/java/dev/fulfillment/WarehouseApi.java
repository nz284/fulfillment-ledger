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
    WarehouseBook(EntityManager em) { this.em=em; }
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
    WarehouseApi(WarehouseBook book) { this.book=book; }

    @PostMapping("/shipments")
    void submit(@RequestBody Shipment s) throws InterruptedException {
        book.accept(s);
        // Commit first: the client may time out while the shipment already exists.
        if (slow.contains(s.warehouse())) Thread.sleep(3500);
    }
    @GetMapping("/shipments/{id}") Shipment lookup(@PathVariable UUID id) { return book.get(id); }
    @PutMapping("/demo/slow/{warehouse}") void slow(@PathVariable String warehouse) { slow.add(warehouse); }
    @DeleteMapping("/demo/slow/{warehouse}") void recover(@PathVariable String warehouse) { slow.remove(warehouse); }
}
