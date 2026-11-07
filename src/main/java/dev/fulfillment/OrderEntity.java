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
    @Version long revision;

    boolean matches(OrderRequest r) {
        return warehouse.equals(r.warehouse()) && sku.equals(r.sku()) && quantity == r.quantity();
    }
    OrderView view() { return new OrderView(id, warehouse, sku, quantity, state); }
}
