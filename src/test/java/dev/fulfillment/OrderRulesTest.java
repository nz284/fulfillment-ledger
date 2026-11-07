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

    @Test void invalidOrdersNeverReachPersistence() {
        Ledger ledger=mock(Ledger.class);
        OrderApi api=new OrderApi(ledger,null,null);
        assertThrows(org.springframework.web.server.ResponseStatusException.class,
            () -> api.create("shop","key",new OrderRequest("A","book",0)));
        verifyNoInteractions(ledger);
    }

    @Test void replayUsesBusinessSequence() {
        UUID id=UUID.randomUUID();
        assertTrue(new Event(id,id,"CONFIRMED").sequence()>new Event(id,id,"UNCONFIRMED").sequence());
    }
}
