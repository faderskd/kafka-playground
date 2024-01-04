package transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Purchase(
        @JsonProperty("purchaseId") String purchaseId,
        @JsonProperty("productId") String productId,
        @JsonProperty("userId") String userId,
        @JsonProperty("quantity") int quantity) {
}
