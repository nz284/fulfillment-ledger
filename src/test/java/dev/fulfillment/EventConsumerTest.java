package dev.fulfillment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.BatchListenerFailedException;
import tools.jackson.databind.ObjectMapper;

class EventConsumerTest {
    final ObjectMapper json = new ObjectMapper();
    final BatchStore batches = mock(BatchStore.class);
    final EventConsumer listener = new EventConsumer(json,batches);

    ConsumerRecord<byte[],byte[]> record(int offset, byte[] value) {
        return new ConsumerRecord<>(EventBus.TOPIC,0,offset,null,value);
    }

    @Test void storesOnlyTheValidPrefixBeforeIdentifyingAPoisonRecord() {
        Event a = new Event(UUID.randomUUID(),UUID.randomUUID(),"ACCEPTED");
        Event c = new Event(UUID.randomUUID(),UUID.randomUUID(),"CONFIRMED");
        var batch = List.of(record(0,json.writeValueAsBytes(a)),record(1,new byte[]{-1}),record(2,json.writeValueAsBytes(c)));
        var failure = assertThrows(BatchListenerFailedException.class, () -> listener.project(batch));
        assertEquals(1,failure.getIndex());
        assertInstanceOf(InvalidEvent.class,failure.getCause());
        verify(batches).project(List.of(a));
        verifyNoMoreInteractions(batches);
    }

    @Test void databaseFailureDuringPrefixCommitIsNotAttributedToThePoisonRecord() {
        Event a = new Event(UUID.randomUUID(),UUID.randomUUID(),"ACCEPTED");
        var outage = new IllegalStateException("database unavailable");
        doThrow(outage).when(batches).dispatch(List.of(a));
        assertSame(outage,assertThrows(IllegalStateException.class, () -> listener.dispatch(
            List.of(record(0,json.writeValueAsBytes(a)),record(1,null)))));
    }

    @Test void rejectsIncompleteAndUnknownEvents() {
        for (var invalid : List.of(new Event(null,UUID.randomUUID(),"CONFIRMED"),
                new Event(UUID.randomUUID(),null,"ACCEPTED"),new Event(UUID.randomUUID(),UUID.randomUUID(),"typo")))
            assertThrows(InvalidEvent.class, () -> listener.decode(json.writeValueAsBytes(invalid)));
    }
}
