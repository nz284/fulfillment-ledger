package dev.fulfillment;

import static org.junit.jupiter.api.Assertions.*;
import static dev.fulfillment.WarehouseCircuits.Operation.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.*;

class WarehouseCircuitsTest {
    @Test void separatesWarehouseAndOperationAndCountsOneFailedRequestPerPermit() {
        var metrics=new SimpleMeterRegistry();
        var circuits=new WarehouseCircuits(metrics, 10, 50, Duration.ofSeconds(10));
        for (int i=0;i<5;i++) {
            try (var p=circuits.reserve("A",SUBMIT)) { p.call(() -> "ok"); }
        }
        for (int i=0;i<5;i++) {
            try (var p=circuits.reserve("A",SUBMIT)) {
                assertThrows(HttpServerErrorException.class,() -> p.call(() -> {
                    throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE);
                }));
            }
        }
        assertNull(circuits.reserve("A",SUBMIT));
        assertEquals(1,metrics.get("fulfillment.warehouse.circuit.denied").tags("warehouse","A","operation","submit").counter().count());
        try (var lookup=circuits.reserve("A",LOOKUP); var west=circuits.reserve("B",SUBMIT)) {
            assertNotNull(lookup); assertNotNull(west);
        }
    }

    @Test void releasesUnusedProbeAndGraduallyRestoresBatchSize() throws InterruptedException {
        var circuits=new WarehouseCircuits(new SimpleMeterRegistry(),1,100,Duration.ofMillis(100));
        try (var p=circuits.reserve("A",SUBMIT)) {
            assertThrows(ResourceAccessException.class,() -> p.call(() -> {throw new ResourceAccessException("timeout");}));
        }
        Thread.sleep(150);
        try (var unused=circuits.reserve("A",SUBMIT)) {
            assertNotNull(unused);
            assertEquals(1,unused.batchLimit());
            assertNull(circuits.reserve("A",SUBMIT));
        }
        try (var probe=circuits.reserve("A",SUBMIT)) { assertEquals(1,probe.batchLimit()); probe.call(() -> "ok"); }
        try (var next=circuits.reserve("A",SUBMIT)) { assertEquals(2,next.batchLimit()); }
    }

    @Test void businessRejectionDoesNotOpenTheCircuit() {
        var circuits=new WarehouseCircuits(new SimpleMeterRegistry(),1,100,Duration.ofSeconds(10));
        try (var p=circuits.reserve("A",SUBMIT)) {
            assertThrows(HttpClientErrorException.class,() -> p.call(() -> {
                throw new HttpClientErrorException(HttpStatus.CONFLICT);
            }));
        }
        try (var next=circuits.reserve("A",SUBMIT)) { assertNotNull(next); }
    }
}
