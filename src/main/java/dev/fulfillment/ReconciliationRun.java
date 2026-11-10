package dev.fulfillment;

import jakarta.persistence.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name="reconciliation_run")
class ReconciliationRun {
    @Id UUID id;
    Instant startsAt;
    Instant endsAt;
    String phase;
    Instant cursorAt;
    UUID cursorId;
    long revision;
    long pages;
    int failures;
    String lastError;
    Instant readyAt;
    RunView view() {
        return new RunView(id,startsAt,endsAt,phase,cursorAt,cursorId,revision,pages,failures,lastError,readyAt);
    }
}

record RunView(UUID id, Instant startsAt, Instant endsAt, String phase, Instant cursorAt, UUID cursorId,
               long revision, long pages, int failures, String lastError, Instant readyAt) {}
record ScanKey(Instant at, UUID id) {
    static Instant instant(Object value) {
        if (value instanceof OffsetDateTime time) return time.toInstant();
        return (Instant)value;
    }
}
record ManifestLine(Instant recordedAt, Shipment shipment) {}
record Evidence(OrderView platform, Shipment warehouse) {}
