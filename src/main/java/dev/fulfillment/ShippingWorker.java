package dev.fulfillment;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
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

    ShippingWorker(Tasks tasks, Ledger ledger, RestClient.Builder builder, MeterRegistry metrics, BatchStore batches,
            @Value("${app.warehouse-batch:false}") boolean batchEnabled,
            @Value("${app.warehouse-url}") String url) {
        this.tasks=tasks; this.ledger=ledger; this.metrics=metrics;
        this.batches=batches; this.batchEnabled=batchEnabled;
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
        factory.setReadTimeout(Duration.ofSeconds(2));
        this.client=builder.baseUrl(url).requestFactory(factory).build();
    }

    @Scheduled(fixedDelayString="${app.poll-ms:5}") void east() { drain("A"); }
    @Scheduled(fixedDelayString="${app.poll-ms:5}") void west() { drain("B"); }

    private void drain(String warehouse) {
        var claims=batches.claim("SHIP_"+warehouse,batchEnabled ? 256 : 1);
        var fresh=claims.stream().filter(t -> t.attempts()==1).toList();
        if (fresh.size()>1) submitBatch(fresh,warehouse);
        for (Lease task:claims) {
            if (fresh.size()>1 && task.attempts()==1) continue;
            OrderView order=ledger.get(task.orderId());
            Shipment shipment=new Shipment(order.id(),warehouse,order.sku(),order.quantity());
            try {
                if (task.attempts()==1 || !exists(shipment)) {
                    client.post().uri("/shipments").body(shipment).retrieve().toBodilessEntity();
                }
                tasks.finish(task,true,"CONFIRMED");
                Audit.info("warehouse.confirmed",order.id(),warehouse);
                metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","confirmed").increment();
            } catch (RestClientException failure) {
                Audit.warn("warehouse.result.unknown",order.id(),failure);
                tasks.finish(task,false,"UNCONFIRMED");
                metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","unknown").increment();
            }
        }
    }

    private void submitBatch(List<Lease> claims,String warehouse) {
        var shipments=batches.shipments(claims);
        try {
            client.post().uri("/shipments/batch").body(shipments).retrieve().toBodilessEntity();
            var confirmed=batches.confirmed(claims);
            confirmed.forEach(id -> Audit.info("warehouse.confirmed",id,warehouse));
            metrics.counter("fulfillment.warehouse.results","warehouse",warehouse,"outcome","confirmed").increment(confirmed.size());
        } catch (RestClientException failure) {
            // Ambiguous results return to individual lookup-before-resubmit recovery.
            for (var task:claims) {
                Audit.warn("warehouse.result.unknown",task.orderId(),failure);
                tasks.finish(task,false,"UNCONFIRMED");
            }
        }
    }

    private boolean exists(Shipment expected) {
        try {
            Shipment found=client.get().uri("/shipments/{id}",expected.id()).retrieve().body(Shipment.class);
            if (!expected.equals(found)) throw new RestClientException("Conflicting warehouse record");
            return true;
        } catch (HttpClientErrorException.NotFound missing) { return false; }
    }
}
