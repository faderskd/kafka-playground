package kafkaplayground.transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Purchase(
        @JsonProperty("purchaseId") String purchaseId,
        @JsonProperty("productId") String productId,
        @JsonProperty("userId") String userId,
        @JsonProperty("userFullName") String userFullName,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("totalPrice") String totalPrice) {
}
