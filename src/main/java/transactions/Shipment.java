package transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Shipment(
        @JsonProperty("shipmentId") String shipmentId,
        @JsonProperty("productId") String productId,
        @JsonProperty("recipient") String recipient,
        @JsonProperty("address") String address,
        @JsonProperty("quantity") int quantity) {
}
