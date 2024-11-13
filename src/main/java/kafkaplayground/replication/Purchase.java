package kafkaplayground.replication;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Purchase(
        @JsonProperty("purchaseId") int purchaseId,
        @JsonProperty("userFullName") String userFullName,
        @JsonProperty("totalPrice") String totalPrice) {
}
