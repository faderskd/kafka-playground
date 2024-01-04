package transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ProductStock(
        @JsonProperty("purchaseId") String purchaseId,
        @JsonProperty("productId") String productId,
        @JsonProperty("diff") int diff) {
}
