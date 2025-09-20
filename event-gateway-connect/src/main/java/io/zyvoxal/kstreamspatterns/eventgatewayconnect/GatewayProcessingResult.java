package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public record GatewayProcessingResult(GatewayProcessingResult.Status status, GatewayEnvelope envelope, String reason, int attempt) {
  enum Status {
    SUCCESS,
    RETRY,
    DLQ
  }
}
