package dev.fulfillment;

import jakarta.persistence.*;
import java.util.UUID;

@Entity
@Table(name="fulfillment_order")
class OrderEntity {
    @Id UUID id;
    String tenant;
    @Column(name="request_key") String requestKey;
    String sku;
    int quantity;
    String warehouse;
    String state;
    @Column(insertable=false,updatable=false) java.time.Instant createdAt;
    @Version long revision;

    OrderView view() { return new OrderView(id, warehouse, sku, quantity, state); }
}
