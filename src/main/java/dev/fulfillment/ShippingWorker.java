package dev.fulfillment;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;
import io.micrometer.core.instrument.MeterRegistry;

@Component
@Profile("worker")
class ShippingWorker {
    private final Tasks tasks;
    private final Ledger ledger;
    private final RestClient client;
    private final MeterRegistry metrics;
    private final BatchStore batches;
    private final boolean batchEnabled;
    private final WarehouseCircuits circuits;

    ShippingWorker(Tasks tasks, Ledger ledger, RestClient.Builder builder, MeterRegistry metrics, BatchStore batches, WarehouseCircuits circuits,
            @Value("${app.warehouse-batch:false}") boolean batchEnabled,
            @Value("${app.warehouse-url}") String url) {
        this.tasks=tasks; this.ledger=ledger; this.metrics=metrics;
        this.batches=batches; this.batchEnabled=batchEnabled; this.circuits=circuits;
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
        factory.setReadTimeout(Duration.ofSeconds(2));
        this.client=builder.baseUrl(url).requestFactory(factory).build();
    }

    @Scheduled(fixedDelayString="${app.poll-ms:5}") void east() { drain("A"); }
    @Scheduled(fixedDelayString="${app.poll-ms:5}") void west() { drain("B"); }

    private void drain(String warehouse) {
        submitFresh(warehouse);
        recoverOne(warehouse);
    }

    private void submitFresh(String warehouse) {
        try (var permit=circuits.reserve(warehouse, WarehouseCircuits.Operation.SUBMIT)) {
            if (permit==null) return;
            var claims=batches.claimFresh(warehouse, batchEnabled ? permit.batchLimit() : 1);
            if (claims.isEmpty()) return;
            var shipments=batches.shipments(claims);
            try {
                permit.call(() -> shipments.size()==1 ? post(shipments.getFirst()) :
                    client.post().uri("/shipments/batch").body(shipments).retrieve().toBodilessEntity());
            } catch (RestClientException failure) {
                // One HTTP failure goes to the breaker; each affected order still needs reconciliation.
                for (var task:claims) uncertain(task,warehouse,failure);
                return;
            }
            var confirmed=batches.confirmed(claims);
            confirmed.forEach(id -> Audit.info("warehouse.confirmed",id,warehouse));
            metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","confirmed")
                .increment(confirmed.size());
        }
    }

    private org.springframework.http.ResponseEntity<Void> post(Shipment shipment) {
        return client.post().uri("/shipments").body(shipment).retrieve().toBodilessEntity();
    }

    private void recoverOne(String warehouse) {
        try (var lookup=circuits.reserve(warehouse, WarehouseCircuits.Operation.LOOKUP)) {
            if (lookup==null) return;
            var claims=batches.claimForLookup(warehouse);
            if (claims.isEmpty()) return;
            var task=claims.getFirst();
            var order=ledger.get(task.orderId());
            var shipment=new Shipment(order.id(), warehouse, order.sku(), order.quantity());
            try {
                if (!lookup.call(() -> exists(shipment))) {
                    try (var submit=circuits.reserve(warehouse, WarehouseCircuits.Operation.SUBMIT)) {
                        if (submit==null) {
                            tasks.finish(task,false,null);
                            return;
                        }
                        submit.call(() -> post(shipment));
                    }
                }
            } catch (RestClientException failure) {
                uncertain(task,warehouse,failure);
                return;
            }
            tasks.finish(task,true,"CONFIRMED");
            Audit.info("warehouse.confirmed",order.id(),warehouse);
            metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","confirmed").increment();
        }
    }

    private void uncertain(Lease task, String warehouse, RestClientException failure) {
        Audit.warn("warehouse.result.unknown",task.orderId(),failure);
        tasks.finish(task,false,"UNCONFIRMED");
        metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","unknown").increment();
    }

    private boolean exists(Shipment expected) {
        try {
            Shipment found=client.get().uri("/shipments/{id}",expected.id()).retrieve().body(Shipment.class);
            if (!expected.equals(found)) throw new RestClientException("Conflicting warehouse record");
            Audit.info("warehouse.lookup.matched",expected.id(),expected.warehouse());
            return true;
        } catch (HttpClientErrorException.NotFound missing) {
            Audit.info("warehouse.lookup.missing",expected.id(),expected.warehouse());
            return false;
        }
    }
}
