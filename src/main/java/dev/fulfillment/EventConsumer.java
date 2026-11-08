package dev.fulfillment;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

class InvalidEvent extends RuntimeException {
    InvalidEvent(String reason) { super(reason); }
}

class MissingOrder extends RuntimeException {
    final UUID orderId;
    MissingOrder(UUID orderId) {
        super("Order not available for dispatch: " + orderId);
        this.orderId = orderId;
    }
}

@Component
class EventConsumer {
    static final Logger TIMINGS = LoggerFactory.getLogger("fulfillment.dispatch.timing");
    private final ObjectMapper mapper;
    private final BatchStore batches;

    EventConsumer(ObjectMapper mapper, BatchStore batches) {
        this.mapper = mapper;
        this.batches = batches;
    }

    Event decode(byte[] value) {
        if (value == null) throw new InvalidEvent("Missing event payload");
        Event event;
        try {
            event = mapper.readValue(value, Event.class);
        } catch (JacksonException invalid) {
            throw new InvalidEvent("Invalid event JSON or UUID: " + invalid.getClass().getSimpleName());
        }
        if (event == null || event.id() == null || event.orderId() == null || event.state() == null)
            throw new InvalidEvent("Event requires id, orderId and state");
        try {
            event.sequence();
        } catch (IllegalArgumentException invalid) {
            throw new InvalidEvent("Unknown fulfillment state");
        }
        return event;
    }

    void dispatch(List<ConsumerRecord<byte[],byte[]>> records) {
        consume(records, batches::dispatch, true);
    }

    void project(List<ConsumerRecord<byte[],byte[]>> records) {
        consume(records, batches::project, false);
    }

    private void consume(List<ConsumerRecord<byte[],byte[]>> records, Consumer<List<Event>> write, boolean dispatch) {
        List<Event> events = new ArrayList<>(records.size());
        for (var record : records) {
            Event event;
            try {
                event = decode(record.value());
            } catch (InvalidEvent invalid) {
                persist(events, write);
                throw new BatchListenerFailedException("Invalid event", invalid, events.size());
            }
            if (dispatch && TIMINGS.isDebugEnabled() && event.state().equals("ACCEPTED"))
                TIMINGS.atDebug().addKeyValue("event","dispatch.received")
                    .addKeyValue("fulfillment_id",event.orderId()).log("dispatch.received");
            events.add(event);
        }
        persist(events, write);
    }

    private void persist(List<Event> events, Consumer<List<Event>> write) {
        if (events.isEmpty()) return;
        try {
            write.accept(events);
        } catch (MissingOrder missing) {
            // The whole transaction rolled back. Commit only the prefix before exposing its offset.
            for (int i = 0; i < events.size(); i++) {
                Event event = events.get(i);
                if (event.state().equals("ACCEPTED") && event.orderId().equals(missing.orderId)) {
                    if (i > 0) write.accept(events.subList(0, i));
                    throw new BatchListenerFailedException("Order unavailable", missing, i);
                }
            }
            throw missing;
        }
    }
}
