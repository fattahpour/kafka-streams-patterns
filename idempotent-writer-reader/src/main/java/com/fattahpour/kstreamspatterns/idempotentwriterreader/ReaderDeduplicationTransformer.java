package com.fattahpour.kstreamspatterns.idempotentwriterreader;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class ReaderDeduplicationTransformer
    implements ValueTransformerWithKeySupplier<String, DeduplicatedEvent, ProcessedEvent> {
  static final String STORE_NAME = "reader-store";
  private static final Map<String, Set<String>> PROCESSED_IDS = new ConcurrentHashMap<>();
  private final PatternMetrics metrics;

  ReaderDeduplicationTransformer(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ValueTransformerWithKey<String, DeduplicatedEvent, ProcessedEvent> get() {
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
      public ProcessedEvent transform(String readOnlyKey, DeduplicatedEvent value) {
        if (value == null) {
          return null;
        }
        if (processedIds.contains(value.eventId())) {
          return null;
        }
        Long existing = store.get(value.eventId());
        if (existing != null) {
          return null;
        }
        store.put(value.eventId(), context.timestamp());
        store.flush();
        context.commit();
        processedIds.add(value.eventId());
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
