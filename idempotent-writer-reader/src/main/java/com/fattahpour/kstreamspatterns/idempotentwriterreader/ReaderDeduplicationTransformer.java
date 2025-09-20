package com.fattahpour.kstreamspatterns.idempotentwriterreader;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class ReaderDeduplicationTransformer
    implements ValueTransformerWithKeySupplier<String, DeduplicatedEvent, ProcessedEvent> {
  static final String STORE_NAME = "reader-store";
  private final PatternMetrics metrics;

  ReaderDeduplicationTransformer(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ValueTransformerWithKey<String, DeduplicatedEvent, ProcessedEvent> get() {
    return new ValueTransformerWithKey<>() {
      private ProcessorContext<String, ProcessedEvent> context;
      private KeyValueStore<String, Long> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext<String, ProcessedEvent> context) {
        this.context = context;
        this.store = (KeyValueStore<String, Long>) context.getStateStore(STORE_NAME);
      }

      @Override
      public ProcessedEvent transform(String readOnlyKey, DeduplicatedEvent value) {
        if (value == null) {
          return null;
        }
        if (store.get(value.eventId()) != null) {
          return null;
        }
        store.put(value.eventId(), context.timestamp());
        addHeader("correlation-id", value.correlationId());
        metrics.markReaderEmitted();
        return new ProcessedEvent(value.eventId(), value.payload(), context.timestamp(), value.correlationId());
      }

      private void addHeader(String name, String value) {
        if (value == null) {
          return;
        }
        context.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
      }

      @Override
      public void close() {}
    };
  }
}
