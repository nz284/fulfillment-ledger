package dev.fulfillment;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.UUID;

class OrderRulesTest {
    @Test void validatesRequestsBeforePersistence() {
        assertTrue(new OrderRequest("A","book",2).valid());
        assertFalse(new OrderRequest("C","book",2).valid());
        assertFalse(new OrderRequest("B"," ",2).valid());
        assertFalse(new OrderRequest("B","book",0).valid());
    }

    @Test void onlyAcceptanceCreatesDispatchTask() {
        Ledger ledger=mock(Ledger.class,withSettings().defaultAnswer(CALLS_REAL_METHODS));
        ledger.dispatch(new Event(UUID.randomUUID(),UUID.randomUUID(),"CONFIRMED"));
        verify(ledger,never()).enqueue(any(),any(),any(),any());
    }

    @Test void replayUsesBusinessSequence() {
        UUID id=UUID.randomUUID();
        assertTrue(new Event(id,id,"CONFIRMED").sequence()>new Event(id,id,"UNCONFIRMED").sequence());
    }
}
