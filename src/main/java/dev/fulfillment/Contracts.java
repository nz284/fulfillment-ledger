package dev.fulfillment;

import java.util.UUID;

record OrderRequest(String warehouse, String sku, int quantity) {
    boolean valid() {
        return ("A".equals(warehouse) || "B".equals(warehouse)) && sku != null
            && !sku.isBlank() && sku.length() <= 120 && quantity > 0;
    }
}
record OrderView(UUID id, String warehouse, String sku, int quantity, String state) {}
record Event(UUID id, UUID orderId, String state) {
    int sequence() { return switch (state) {
        case "ACCEPTED" -> 1;
        case "UNCONFIRMED" -> 2;
        case "CONFIRMED" -> 3;
        default -> throw new IllegalArgumentException("Unknown state " + state);
    }; }
}
record Lease(UUID id, UUID orderId, String payload, long token, int attempts) {}
record Shipment(UUID id, String warehouse, String sku, int quantity) {}
