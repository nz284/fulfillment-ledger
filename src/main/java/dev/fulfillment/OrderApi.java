package dev.fulfillment;

import java.util.*;
import java.time.Duration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
class OrderApi {
    private final Ledger ledger;
    private final StringRedisTemplate cache;
    private final java.util.concurrent.Semaphore fallback = new java.util.concurrent.Semaphore(8);
    OrderApi(Ledger ledger, StringRedisTemplate cache) { this.ledger = ledger; this.cache = cache; }

    @PostMapping("/orders")
    ResponseEntity<OrderView> create(@RequestHeader("X-Tenant") String tenant,
            @RequestHeader("Idempotency-Key") String key, @RequestBody OrderRequest request) {
        if (tenant.isBlank() || tenant.length()>100 || key.isBlank() || key.length()>160 || !request.valid())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid tenant, key, warehouse, SKU or quantity");
        OrderView view = ledger.accept(tenant,key,request);
        return ResponseEntity.accepted().body(view);
    }

    @GetMapping("/orders/{id}")
    OrderView get(@PathVariable UUID id, @RequestHeader("X-Tenant") String tenant) {
        return ledger.forTenant(id,tenant);
    }

    @GetMapping("/orders/{id}/timeline")
    List<?> timeline(@PathVariable UUID id, @RequestHeader("X-Tenant") String tenant) {
        ledger.forTenant(id,tenant);
        return ledger.timeline(id);
    }

    @GetMapping("/warehouses/{id}")
    Map<String,String> warehouse(@PathVariable String id) {
        if (!Set.of("A","B").contains(id)) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        String key = "warehouse:v1:" + id;
        boolean available = true;
        try {
            String hit = cache.opsForValue().get(key);
            if (hit != null) return Map.of("id",id,"name",hit);
        } catch (DataAccessException unavailable) { available = false; }
        if (!fallback.tryAcquire()) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        try {
            String name = ledger.directory(id);
            if (available) {
                try { cache.opsForValue().set(key,name,Duration.ofSeconds(300 + new Random().nextInt(60))); }
                catch (DataAccessException ignored) { /* Database result is still usable. */ }
            }
            return Map.of("id",id,"name",name);
        } finally { fallback.release(); }
    }
}
