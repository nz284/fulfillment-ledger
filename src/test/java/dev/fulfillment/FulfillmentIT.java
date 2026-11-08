package dev.fulfillment;

import com.github.tomakehurst.wiremock.WireMockServer;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.*;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;

@SpringBootTest
@ActiveProfiles({"api","worker"})
class FulfillmentIT {
    static final PostgreSQLContainer db=new PostgreSQLContainer("postgres:17-alpine");
    static final KafkaContainer kafka=new KafkaContainer("apache/kafka:4.1.1");
    static final GenericContainer<?> redis=new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
    static final WireMockServer warehouse=new WireMockServer(options().dynamicPort());

    @DynamicPropertySource static void configure(DynamicPropertyRegistry p) {
        db.start(); kafka.start(); redis.start(); warehouse.start();
        p.add("spring.datasource.url",db::getJdbcUrl);
        p.add("spring.datasource.username",db::getUsername);
        p.add("spring.datasource.password",db::getPassword);
        p.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
        p.add("spring.data.redis.host",redis::getHost);
        p.add("spring.data.redis.port",redis::getFirstMappedPort);
        p.add("app.warehouse-url",warehouse::baseUrl);
        p.add("spring.task.scheduling.pool.size",()->4);
        p.add("app.poll-ms",()->100);
    }

    @Autowired Ledger ledger;
    @Autowired OrderAdmissions admissions;
    @Autowired OrderApi api;
    @Autowired WarehouseBook book;
    @Autowired BatchStore batches;
    @Autowired jakarta.persistence.EntityManager em;

    @Test
    @org.springframework.transaction.annotation.Transactional
    void reclaimedBatchRejectsStaleWorkerAndProjectsOnce() {
        var key=new OrderKey("batch-test","1");
        var request=new Admission(key,new OrderRequest("B","sku",3));
        var id=ledger.acceptBatch(List.of(request,request)).get(key).id();
        assertEquals(7,id.version());
        var publish=batches.claim("OUTBOX",1);
        var stale=publish.stream().map(l -> new Lease(l.id(),l.orderId(),l.payload(),l.token()-1,l.attempts())).toList();
        batches.published(stale);
        assertNotNull(em.find(WorkItem.class,publish.getFirst().id()));
        batches.published(publish);
        em.clear();
        assertNull(em.find(WorkItem.class,publish.getFirst().id()));
        assertEquals(1L,((Number)em.createNativeQuery("SELECT count(*) FROM history WHERE order_id=:id")
            .setParameter("id",id).getSingleResult()).longValue());
        var event=new Event(UUID.randomUUID(),id,"ACCEPTED");
        batches.dispatch(List.of(event,event));
        var old=batches.claim("SHIP_B",1);
        assertEquals(id,old.getFirst().orderId());
        em.createNativeQuery("UPDATE work_item SET available_at=now() WHERE id=:id")
            .setParameter("id",old.getFirst().id()).executeUpdate();
        var current=batches.claim("SHIP_B",1);
        assertTrue(batches.confirmed(old).isEmpty());
        assertEquals(List.of(id),batches.confirmed(current));
        assertTrue(batches.confirmed(current).isEmpty());
        var confirmed=new Event(UUID.randomUUID(),id,"CONFIRMED");
        batches.project(List.of(confirmed,event,confirmed));
        assertEquals(List.of("ACCEPTED","CONFIRMED"),ledger.timeline(id));
    }
    @Test void warehouseBatchReplaysAndRollsBackConflictingPayloads() {
        var first=new Shipment(UUID.randomUUID(),"B","book",1);
        var second=new Shipment(UUID.randomUUID(),"B","lamp",2);
        book.acceptBatch(List.of(first,second));
        book.acceptBatch(List.of(second,first));
        var third=new Shipment(UUID.randomUUID(),"B","pen",1);
        var conflict=new Shipment(first.id(),"B","other",1);
        assertThrows(ResponseStatusException.class,()->book.acceptBatch(List.of(third,conflict)));
        assertThrows(ResponseStatusException.class,()->book.get(third.id()));
        assertEquals(first,book.get(first.id()));
        assertEquals(second,book.get(second.id()));
    }
    @Test void concurrentRetriesAndLostWarehouseResponse() throws Exception {
        var request=new OrderRequest("A","book",2);
        warehouse.stubFor(post(urlEqualTo("/shipments")).willReturn(aResponse().withFixedDelay(3200).withStatus(200)));
        var gate=new CountDownLatch(1);
        Set<UUID> ids=new HashSet<>();
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<UUID>> calls=new ArrayList<>();
            for(int i=0;i<12;i++) calls.add(pool.submit(()->{gate.await();return admissions.accept("shop","same",request).id();}));
            gate.countDown();
            for(var call:calls) ids.add(call.get(15,TimeUnit.SECONDS));
        }
        assertEquals(1,ids.size());
        UUID id=ids.iterator().next();
        warehouse.stubFor(get(urlEqualTo("/shipments/"+id)).willReturn(okJson("""
            {"id":"%s","warehouse":"A","sku":"book","quantity":2}
            """.formatted(id))));
        assertThrows(ResponseStatusException.class,()->admissions.accept("shop","same",new OrderRequest("A","other",2)));
        assertThrows(ResponseStatusException.class,()->ledger.forTenant(id,"another-shop"));
        await().atMost(Duration.ofSeconds(90)).untilAsserted(()->assertEquals(
            List.of("ACCEPTED","UNCONFIRMED","CONFIRMED"),ledger.timeline(id)));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(()->warehouse.verify(1,postRequestedFor(urlEqualTo("/shipments"))));
        assertEquals("CONFIRMED",ledger.get(id).state());
        assertEquals(api.warehouse("A"),api.warehouse("A"));
    }

    @AfterAll static void stopWarehouse() { warehouse.stop(); }
}
