package com.fattahpour.kstreamspatterns.idempotentwriterreader;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class WriterDeduplicationTransformer
    implements ValueTransformerWithKeySupplier<String, InboundEvent, DeduplicatedEvent> {
  static final String STORE_NAME = "writer-store";
  private static final Map<String, Set<String>> PROCESSED_IDS = new ConcurrentHashMap<>();
  private final PatternMetrics metrics;

  WriterDeduplicationTransformer(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ValueTransformerWithKey<String, InboundEvent, DeduplicatedEvent> get() {
    return new ValueTransformerWithKey<>() {
      private ProcessorContext context;
      private KeyValueStore<String, Long> store;
      private Set<String> processedIds;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, Long>) context.getStateStore(STORE_NAME);
        this.processedIds = PROCESSED_IDS.computeIfAbsent(context.applicationId(), id -> ConcurrentHashMap.newKeySet());
      }

      @Override
      public DeduplicatedEvent transform(String readOnlyKey, InboundEvent value) {
        if (value == null) {
          return null;
        }
        metrics.markWriterProcessed();
        if (processedIds.contains(value.eventId()) || store.get(value.eventId()) != null) {
          return null;
        }
        store.put(value.eventId(), context.timestamp());
        store.flush();
        context.commit();
        addHeader("correlation-id", value.correlationId());
        addHeader("causation-id", value.eventId());
        processedIds.add(value.eventId());
        metrics.markWriterEmitted();
        return new DeduplicatedEvent(value.eventId(), value.payload(), value.correlationId());
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
