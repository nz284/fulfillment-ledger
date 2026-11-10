package dev.fulfillment;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@Profile("reconciler")
class ReconciliationWorker {
    private final ReconciliationStore store;
    private final RestClient warehouse;
    private final int pageSize;
    private final boolean autoSchedule;

    ReconciliationWorker(ReconciliationStore store,RestClient.Builder builder,
            @Value("${app.warehouse-url}") String url,
            @Value("${app.reconciliation.page-size:200}") int pageSize,
            @Value("${app.reconciliation.auto-schedule:true}") boolean autoSchedule) {
        if (pageSize<1 || pageSize>500) throw new IllegalArgumentException("Page size must be 1..500");
        this.store=store; this.pageSize=pageSize; this.autoSchedule=autoSchedule;
        var http=new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
        http.setReadTimeout(Duration.ofSeconds(5));
        warehouse=builder.baseUrl(url).requestFactory(http).build();
    }

    @Scheduled(fixedDelayString="${app.reconciliation.poll-ms:1000}")
    void tick() {
        if (autoSchedule) store.scheduleClosedWindow();
        for (int i=0;i<32;i++) {
            var next=store.next();
            if (next.isEmpty()) return;
            var run=next.get();
            try { page(run); }
            catch (RuntimeException failure) {
                store.failed(run,failure);
                LoggerFactory.getLogger(getClass()).warn("Reconciliation run {} failed at {} cursor {}; retry scheduled",
                    run.id(),run.phase(),run.cursorId(),failure);
                return;
            }
        }
    }

    private void page(RunView run) {
        List<ScanKey> keys;
        Map<UUID,Shipment> found=new HashMap<>();
        if (run.phase().equals("ORDERS")) {
            keys=store.orderKeys(run,pageSize);
            if (!keys.isEmpty()) {
                var ids=keys.stream().map(ScanKey::id).toList();
                var shipments=warehouse.post().uri("/shipments/lookup").body(ids).retrieve().body(Shipment[].class);
                var expected=new HashSet<>(ids);
                for (var shipment:Objects.requireNonNull(shipments)) {
                    checkShipment(shipment);
                    if (!expected.remove(shipment.id())) throw new IllegalStateException("Unexpected lookup ID");
                    found.put(shipment.id(),shipment);
                }
            }
        } else {
            var lines=warehouse.get().uri(uri -> {
                var u=uri.path("/shipments/manifest").queryParam("from",run.startsAt()).queryParam("to",run.endsAt())
                    .queryParam("limit",pageSize);
                if (run.cursorId()!=null) u.queryParam("afterTime",run.cursorAt()).queryParam("afterId",run.cursorId());
                return u.build();
            }).retrieve().body(ManifestLine[].class);
            if (Objects.requireNonNull(lines).length>pageSize) throw new IllegalStateException("Oversized manifest page");
            keys=new ArrayList<>(lines.length);
            var previous=run.cursorId()==null ? null : new ScanKey(run.cursorAt(),run.cursorId());
            for (var line:lines) {
                checkShipment(line.shipment());
                var key=new ScanKey(line.recordedAt(),line.shipment().id());
                if (key.at().isBefore(run.startsAt()) || !key.at().isBefore(run.endsAt()) ||
                    (previous!=null && (key.at().isBefore(previous.at()) || (key.at().equals(previous.at())
                        && key.id().toString().compareTo(previous.id().toString())<=0))))
                    throw new IllegalStateException("Manifest cursor outside requested range or out of order");
                if (found.put(key.id(),line.shipment())!=null) throw new IllegalStateException("Duplicate manifest ID");
                keys.add(key); previous=key;
            }
        }
        store.apply(run,keys,found,keys.size()<pageSize);
    }

    private void checkShipment(Shipment s) {
        if (s==null || s.id()==null || s.warehouse()==null || s.sku()==null)
            throw new IllegalStateException("Incomplete warehouse record");
    }
}
