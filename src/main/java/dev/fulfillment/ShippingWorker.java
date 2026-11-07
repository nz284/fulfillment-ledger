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

    ShippingWorker(Tasks tasks, Ledger ledger, RestClient.Builder builder, MeterRegistry metrics,
            @Value("${app.warehouse-url}") String url) {
        this.tasks=tasks; this.ledger=ledger; this.metrics=metrics;
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
        factory.setReadTimeout(Duration.ofSeconds(2));
        this.client=builder.baseUrl(url).requestFactory(factory).build();
    }

    @Scheduled(fixedDelayString="${app.poll-ms:200}") void east() { drain("A"); }
    @Scheduled(fixedDelayString="${app.poll-ms:200}") void west() { drain("B"); }

    private void drain(String warehouse) {
        for (int i=0; i<20; i++) {
            var next=tasks.claim("SHIP_"+warehouse);
            if (next.isEmpty()) return;
            Lease task=next.get();
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
                return;
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
