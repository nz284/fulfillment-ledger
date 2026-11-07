package dev.fulfillment;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="work_item")
class WorkItem {
    @Id UUID id;
    UUID orderId;
    String kind;
    String payload;
    Instant availableAt;
    long token;
    int attempts;
    boolean done;
}
