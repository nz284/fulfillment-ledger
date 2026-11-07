package dev.fulfillment;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Audit {
    private static final Logger log = LoggerFactory.getLogger("fulfillment.events");
    static void info(String event, UUID order, String warehouse) {
        log.atInfo().addKeyValue("event",event).addKeyValue("fulfillment_id",order)
            .addKeyValue("warehouse_id",warehouse).log(event);
    }
    static void warn(String event, UUID order, Exception failure) {
        log.atWarn().addKeyValue("event",event).addKeyValue("fulfillment_id",order)
            .addKeyValue("error_type",failure.getClass().getSimpleName()).setCause(failure).log(event);
    }
}
