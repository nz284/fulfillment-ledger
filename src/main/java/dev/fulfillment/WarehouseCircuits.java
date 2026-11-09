package dev.fulfillment;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

@Component
@Profile("worker")
class WarehouseCircuits {
    enum Operation { SUBMIT, LOOKUP }
    private record Key(String warehouse, Operation operation) {}
    private record Lane(CircuitBreaker breaker, AtomicInteger batchLimit) {}
    private final Map<Key,Lane> lanes = new HashMap<>();

    WarehouseCircuits(MeterRegistry metrics, @Value("${app.circuit.window:10}") int window,
            @Value("${app.circuit.failure-rate:50}") float failureRate,
            @Value("${app.circuit.open-duration:10s}") Duration cooldown) {
        var config = CircuitBreakerConfig.custom().slidingWindowSize(window).minimumNumberOfCalls(window)
            .failureRateThreshold(failureRate).waitDurationInOpenState(cooldown)
            .permittedNumberOfCallsInHalfOpenState(1)
            .ignoreException(e -> !(e instanceof ResourceAccessException ||
                (e instanceof RestClientResponseException response && response.getStatusCode().is5xxServerError())))
            .build();
        for (String warehouse : new String[]{"A","B"}) {
            for (Operation operation : Operation.values()) {
                String name = operation.name().toLowerCase(java.util.Locale.ROOT);
                var breaker = CircuitBreaker.of(warehouse + "." + name, config);
                var lane = new Lane(breaker, new AtomicInteger(256));
                lanes.put(new Key(warehouse, operation), lane);
                var denied = metrics.counter("fulfillment.warehouse.circuit.denied","warehouse",warehouse,"operation",name);
                Gauge.builder("fulfillment.warehouse.circuit.state", breaker, b -> switch (b.getState()) {
                    case OPEN, FORCED_OPEN -> 1;
                    case HALF_OPEN -> 2;
                    default -> 0;
                }).tags("warehouse",warehouse,"operation",name).register(metrics);
                breaker.getEventPublisher().onCallNotPermitted(e -> denied.increment())
                    .onStateTransition(e -> {
                        var state = e.getStateTransition().getToState();
                        if (state == CircuitBreaker.State.OPEN) lane.batchLimit().set(1);
                        LoggerFactory.getLogger("fulfillment.events").atInfo()
                            .addKeyValue("event","warehouse.circuit.transition").addKeyValue("warehouse_id",warehouse)
                            .addKeyValue("operation",name).addKeyValue("state",state.name()).log("warehouse.circuit.transition");
                    });
            }
        }
    }

    /** Reserve before leasing. Closing an unused half-open permit lets a later poll probe. */
    Permit reserve(String warehouse, Operation operation) {
        Lane lane = lanes.get(new Key(warehouse, operation));
        return lane.breaker().tryAcquirePermission() ? new Permit(lane) : null;
    }

    static final class Permit implements AutoCloseable {
        private final Lane lane;
        private boolean consumed;
        Permit(Lane lane) { this.lane = lane; }
        int batchLimit() { return lane.batchLimit().get(); }

        <T> T call(Supplier<T> http) {
            if (consumed) throw new IllegalStateException("Circuit permit already used");
            consumed = true;
            long start = System.nanoTime();
            T result;
            try {
                result = http.get();
            } catch (RuntimeException e) {
                lane.breaker().onError(System.nanoTime()-start, TimeUnit.NANOSECONDS, e);
                throw e;
            }
            lane.breaker().onSuccess(System.nanoTime()-start, TimeUnit.NANOSECONDS);
            if (lane.batchLimit().get() < 256)
                lane.batchLimit().updateAndGet(n -> Math.min(256, n * 2));
            return result;
        }

        @Override public void close() {
            if (!consumed) lane.breaker().releasePermission();
        }
    }
}
