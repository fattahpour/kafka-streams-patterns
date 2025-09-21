package com.fattahpour.kstreamspatterns.projectiontablettl;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

final class ProjectionTableTransformer
    implements TransformerSupplier<String, ProjectionUpdate, KeyValue<String, ProjectionResult>> {
  static final String STORE_NAME = "projection-table-store";
  private final long ttlMs;
  private final PatternMetrics metrics;

  ProjectionTableTransformer(long ttlMs, PatternMetrics metrics) {
    this.ttlMs = ttlMs;
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, ProjectionUpdate, KeyValue<String, ProjectionResult>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, ProjectionState> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, ProjectionState>) context.getStateStore(STORE_NAME);
        context.schedule(
            java.time.Duration.ofMillis(ttlMs), PunctuationType.WALL_CLOCK_TIME, this::expireEntries);
      }

      @Override
      public KeyValue<String, ProjectionResult> transform(String key, ProjectionUpdate value) {
        if (key == null || value == null) {
          return null;
        }
        ProjectionState current = store.get(key);
        if (current != null && current.version() >= value.version()) {
          metrics.markSkipped();
          return null;
        }
        long updatedAt = Math.max(context.timestamp(), value.eventTimestamp());
        ProjectionState newState = new ProjectionState(value.version(), value.payload(), updatedAt);
        store.put(key, newState);
        metrics.markUpsert();
        ProjectionView view =
            new ProjectionView(key, value.version(), value.payload(), updatedAt);
        return KeyValue.pair(key, new ProjectionResult(view, null));
      }

      private void expireEntries(long timestamp) {
        try (KeyValueIterator<String, ProjectionState> iterator = store.all()) {
          while (iterator.hasNext()) {
            KeyValue<String, ProjectionState> entry = iterator.next();
            ProjectionState state = entry.value;
            if (timestamp - state.updatedAt() >= ttlMs) {
              store.delete(entry.key);
              metrics.markExpired();
              ExpiredProjection expired =
                  new ExpiredProjection(entry.key, state.version(), timestamp);
              context.forward(entry.key, new ProjectionResult(null, expired));
            }
          }
        }
      }

      @Override
      public void close() {}
    };
  }
}
