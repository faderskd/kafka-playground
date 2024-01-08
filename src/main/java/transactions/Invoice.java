package transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Invoice(
        @JsonProperty("userId") String userId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("totalPrice") String totalPrice) {
}
