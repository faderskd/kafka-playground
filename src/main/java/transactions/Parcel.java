package transactions;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Parcel(
        @JsonProperty("parcelId") String parcelId,
        @JsonProperty("productId") String productId,
        @JsonProperty("userId") String userId,
        @JsonProperty("quantity") int quantity) {
}
