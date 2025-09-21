package com.fattahpour.kstreamspatterns.sagaorchestration;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

final class SagaTransformer implements ValueTransformerWithKeySupplier<String, OrderCommand, SagaOutcome> {
  private final PatternMetrics metrics;

  SagaTransformer(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ValueTransformerWithKey<String, OrderCommand, SagaOutcome> get() {
    return new ValueTransformerWithKey<>() {
      @Override
      public void init(ProcessorContext context) {}

      @Override
      public SagaOutcome transform(String readOnlyKey, OrderCommand value) {
        metrics.markStarted();
        List<SagaEvent> events = new ArrayList<>();
        events.add(new SagaEvent(value.orderId(), "INVENTORY_RESERVED", value.payload()));
        if (value.failPayment()) {
          events.add(new SagaEvent(value.orderId(), "PAYMENT_FAILED", null));
          events.add(new SagaEvent(value.orderId(), "ORDER_FAILED", null));
          metrics.markCompensated();
          CompensationEvent compensation =
              new CompensationEvent(value.orderId(), "RELEASE_INVENTORY");
          return new SagaOutcome(events, compensation, null);
        }
        events.add(new SagaEvent(value.orderId(), "PAYMENT_COMPLETED", null));
        events.add(new SagaEvent(value.orderId(), "ORDER_COMPLETED", value.payload()));
        metrics.markCompleted();
        return new SagaOutcome(events, null, null);
      }

      @Override
      public void close() {}
    };
  }
}
