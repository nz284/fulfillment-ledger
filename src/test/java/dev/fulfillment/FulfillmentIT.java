package dev.fulfillment;

import com.github.tomakehurst.wiremock.WireMockServer;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
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
        warehouse.verify(1,postRequestedFor(urlEqualTo("/shipments")).withRequestBody(matchingJsonPath("$.id",equalTo(id.toString()))));
        assertEquals("CONFIRMED",ledger.get(id).state());
        assertEquals(api.warehouse("A"),api.warehouse("A"));
    }

    @Autowired TransactionTemplate transactions;
    @Autowired ConsumerFactory<byte[],byte[]> readers;
    @Autowired ReplayStore replays;
    @Autowired ObjectMapper json;
    @Autowired MeterRegistry metrics;

    private UUID seedOrder() {
        UUID id = UUID.randomUUID();
        transactions.executeWithoutResult(tx -> em.createNativeQuery("""
            INSERT INTO fulfillment_order(id,tenant,request_key,sku,quantity,warehouse,state)
            VALUES (:id,'dlq',:key,'book',1,'B','ACCEPTED')
            """).setParameter("id",id).setParameter("key",id.toString()).executeUpdate());
        return id;
    }

    private long tasksFor(UUID order) {
        return ((Number)em.createNativeQuery("SELECT count(*) FROM work_item WHERE order_id=:id AND kind='SHIP_B'")
            .setParameter("id",order).getSingleResult()).longValue();
    }

    private KafkaProducer<byte[],byte[]> sender() {
        return new KafkaProducer<>(Map.of("bootstrap.servers",kafka.getBootstrapServers()),
            new ByteArraySerializer(),new ByteArraySerializer());
    }

    private Future<RecordMetadata> send(KafkaProducer<byte[],byte[]> p,int partition,UUID order,byte[] payload) {
        return p.send(new ProducerRecord<>(EventBus.TOPIC,partition,order.toString().getBytes(StandardCharsets.UTF_8),payload));
    }

    private ConsumerRecord<byte[],byte[]> dead(String topic,int partition,long sourceOffset) {
        var tp = new TopicPartition(topic,partition);
        try (var reader = readers.createConsumer("test-inspect",null,null)) {
            reader.assign(List.of(tp));
            reader.seekToBeginning(List.of(tp));
            long deadline = System.nanoTime()+Duration.ofSeconds(30).toNanos();
            while (System.nanoTime()<deadline) {
                for (var record : reader.poll(Duration.ofMillis(250))) {
                    byte[] bytes=record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value();
                    if (ByteBuffer.wrap(bytes).getLong()==sourceOffset) return record;
                }
            }
        }
        throw new AssertionError("Missing dead letter at source offset "+sourceOffset);
    }

    @Test void isolatesPoisonAndReplaysOnlyTheSelectedProjection() throws Exception {
        UUID a=seedOrder(), b=seedOrder(), c=seedOrder();
        byte[] poison={(byte)0xff,0,42};
        long offset;
        try (var p=sender()) {
            send(p,1,a,json.writeValueAsBytes(new Event(UUID.randomUUID(),a,"ACCEPTED")));
            var bad=send(p,1,b,poison);
            send(p,1,c,json.writeValueAsBytes(new Event(UUID.randomUUID(),c,"ACCEPTED"))).get();
            offset=bad.get().offset();
        }
        var dispatch=dead(DeadLetters.DISPATCH,1,offset);
        var timeline=dead(DeadLetters.TIMELINE,1,offset);
        assertArrayEquals(poison,dispatch.value());
        assertArrayEquals(poison,timeline.value());
        assertArrayEquals(b.toString().getBytes(StandardCharsets.UTF_8),timeline.key());
        assertNotNull(timeline.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(()->{
            assertEquals(1,tasksFor(a)); assertEquals(1,tasksFor(c));
            assertTrue(ledger.timeline(a).contains("ACCEPTED")); assertTrue(ledger.timeline(c).contains("ACCEPTED"));
        });
        byte[] repair=json.writeValueAsBytes(new Event(UUID.randomUUID(),b,"ACCEPTED"));
        try(var reader=readers.createConsumer("test-replay",null,null)) {
            var selected=ReplayCommand.read(reader,new TopicPartition(timeline.topic(),1),timeline.offset());
            assertTrue(replays.replay(selected,repair));
            assertFalse(replays.replay(selected,repair));
        }
        assertThrows(IllegalStateException.class,()->replays.replay(timeline,
            json.writeValueAsBytes(new Event(UUID.randomUUID(),b,"ACCEPTED"))));
        assertEquals(List.of("ACCEPTED"),ledger.timeline(b));
        assertEquals(0,tasksFor(b));
        warehouse.verify(0,postRequestedFor(urlEqualTo("/shipments")).withRequestBody(matchingJsonPath("$.id",equalTo(b.toString()))));
        assertTrue(replays.replay(dispatch,repair));
        assertFalse(replays.replay(dispatch,repair));
        assertEquals(1,tasksFor(b));
    }

    private void maxMessageSize(Admin admin,ConfigResource resource,AlterConfigOp.OpType type,String value) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource,List.of(new AlterConfigOp(new ConfigEntry("max.message.bytes",value),type))))
            .all().get(10,TimeUnit.SECONDS);
    }

    @Test void failedDltAcknowledgementRetainsTheConsumerOffset() throws Exception {
        UUID badId=UUID.randomUUID(), after=UUID.randomUUID();
        var tp=new TopicPartition(EventBus.TOPIC,3);
        String group="fulfillment-timeline-v1";
        var failures=metrics.counter("fulfillment.dlt.publish.errors","group",group);
        double initial=failures.count();
        try(var admin=Admin.create(Map.of("bootstrap.servers",kafka.getBootstrapServers())); var producer=sender()) {
            var resource=new ConfigResource(ConfigResource.Type.TOPIC,DeadLetters.TIMELINE);
            long offset;
            maxMessageSize(admin,resource,AlterConfigOp.OpType.SET,"2048");
            try {
                byte[] poison=new byte[64000];
                new Random(42).nextBytes(poison);
                offset=send(producer,3,badId,poison).get().offset();
                send(producer,3,after,json.writeValueAsBytes(new Event(UUID.randomUUID(),after,"CONFIRMED"))).get();
                await().atMost(Duration.ofSeconds(30)).until(()->failures.count()>initial);
                var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(tp);
                assertTrue(committed==null || committed.offset()<=offset);
                assertTrue(ledger.timeline(after).isEmpty());
            } finally {
                maxMessageSize(admin,resource,AlterConfigOp.OpType.DELETE,null);
            }
            dead(DeadLetters.TIMELINE,3,offset);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertEquals(List.of("CONFIRMED"),ledger.timeline(after)));
        }
    }

    @Test void missingOrderGetsDispatchOnlyDeadLetterAfterRetries() throws Exception {
        UUID missing=UUID.randomUUID();
        byte[] bytes=json.writeValueAsBytes(new Event(UUID.randomUUID(),missing,"ACCEPTED"));
        try(var p=sender()) {
            long start=System.nanoTime();
            long offset=send(p,2,missing,bytes).get().offset();
            var entry=dead(DeadLetters.DISPATCH,2,offset);
            assertTrue(System.nanoTime()-start>=Duration.ofSeconds(2).toNanos());
            assertThrows(MissingOrder.class,()->replays.replay(entry,null));
            assertEquals(0L,em.createNativeQuery("SELECT count(*) FROM dead_letter_replay WHERE order_id=:id",Long.class)
                .setParameter("id",missing).getSingleResult());
            assertEquals(List.of("ACCEPTED"),ledger.timeline(missing));
        }
    }

    @AfterAll static void stopWarehouse() { warehouse.stop(); }
}
