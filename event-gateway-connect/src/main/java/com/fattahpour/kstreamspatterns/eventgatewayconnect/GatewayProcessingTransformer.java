package com.fattahpour.kstreamspatterns.eventgatewayconnect;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ProcessorContext;

final class GatewayProcessingTransformer
    implements ValueTransformerWithKeySupplier<String, GatewayEnvelope, GatewayProcessingResult> {
  private final GatewayProcessor processor;

  GatewayProcessingTransformer(GatewayProcessor processor) {
    this.processor = processor;
  }

  @Override
  public ValueTransformerWithKey<String, GatewayEnvelope, GatewayProcessingResult> get() {
    return new ValueTransformerWithKey<>() {
      private ProcessorContext<String, GatewayProcessingResult> context;

      @Override
      public void init(ProcessorContext<String, GatewayProcessingResult> context) {
        this.context = context;
      }

      @Override
      public GatewayProcessingResult transform(String readOnlyKey, GatewayEnvelope value) {
        if (value == null) {
          return null;
        }
        context.headers().add("correlation-id", value.correlationId().getBytes(StandardCharsets.UTF_8));
        context.headers().add("causation-id", value.id().getBytes(StandardCharsets.UTF_8));
        int attempt = readAttempt();
        GatewayProcessor.ProcessingOutcome outcome = processor.evaluate(value, attempt);
        if (outcome.status() == GatewayProcessingResult.Status.RETRY) {
          setHeader("retry-attempt", Integer.toString(outcome.nextAttempt()));
          setHeader("retry-backoff-ms", Long.toString(processor.backoff().toMillis()));
        } else {
          removeHeader("retry-backoff-ms");
        }
        return new GatewayProcessingResult(outcome.status(), value, outcome.reason(), outcome.nextAttempt());
      }

      private void setHeader(String name, String value) {
        removeHeader(name);
        context.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
      }

      private void removeHeader(String name) {
        Header header = context.headers().lastHeader(name);
        if (header != null) {
          context.headers().remove(name, header.value());
        }
      }

      private int readAttempt() {
        Header header = context.headers().lastHeader("retry-attempt");
        if (header == null) {
          return 0;
        }
        try {
          return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
          return 0;
        }
      }

      @Override
      public void close() {}
    };
  }
}
