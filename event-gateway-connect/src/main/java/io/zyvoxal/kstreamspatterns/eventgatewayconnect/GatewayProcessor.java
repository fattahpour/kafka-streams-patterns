package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

import java.time.Duration;

final class GatewayProcessor {
  private final int maxRetries;
  private final Duration backoff;
  private final PatternMetrics metrics;

  GatewayProcessor(int maxRetries, Duration backoff, PatternMetrics metrics) {
    this.maxRetries = maxRetries;
    this.backoff = backoff;
    this.metrics = metrics;
  }

  ProcessingOutcome evaluate(GatewayEnvelope envelope, int attempt) {
    metrics.markProcessed();
    if (envelope.payload().contains("TEMP_ERROR")) {
      if (attempt + 1 > maxRetries) {
        metrics.markDlq();
        return new ProcessingOutcome(GatewayProcessingResult.Status.DLQ, "retry-exhausted", attempt);
      }
      metrics.markRetry();
      return new ProcessingOutcome(GatewayProcessingResult.Status.RETRY, "temporary-error", attempt + 1);
    }
    if (envelope.payload().contains("FATAL_ERROR")) {
      metrics.markDlq();
      return new ProcessingOutcome(GatewayProcessingResult.Status.DLQ, "fatal-error", attempt);
    }
    metrics.markSuccess();
    return new ProcessingOutcome(GatewayProcessingResult.Status.SUCCESS, "ok", attempt);
  }

  Duration backoff() {
    return backoff;
  }

  record ProcessingOutcome(GatewayProcessingResult.Status status, String reason, int nextAttempt) {}
}
