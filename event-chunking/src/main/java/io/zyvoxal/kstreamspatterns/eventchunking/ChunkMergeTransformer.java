package io.zyvoxal.kstreamspatterns.eventchunking;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class ChunkMergeTransformer implements TransformerSupplier<String, EventChunk, KeyValue<String, ChunkMergeResult>> {
  static final String STORE_NAME = "chunk-accumulator-store";
  private final Duration timeout;
  private final PatternMetrics metrics;

  ChunkMergeTransformer(Duration timeout, PatternMetrics metrics) {
    this.timeout = timeout;
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, EventChunk, KeyValue<String, ChunkMergeResult>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, ChunkAccumulatorState> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, ChunkAccumulatorState>) context.getStateStore(STORE_NAME);
        context.schedule(timeout, PunctuationType.WALL_CLOCK_TIME, this::expire); // cleanup
      }

      @Override
      public KeyValue<String, ChunkMergeResult> transform(String key, EventChunk value) {
        ChunkAccumulatorState state = store.get(key);
        if (state == null) {
          state = new ChunkAccumulatorState(value.id(), value.totalChunks(), context.timestamp(), value.correlationId());
        }
        if (state.hasFragment(value.sequence())) {
          metrics.markDuplicate();
          store.put(key, state);
          return null;
        }
        state.addFragment(value.sequence(), value.fragment(), context.timestamp());
        store.put(key, state);
        if (state.isComplete()) {
          store.delete(key);
          metrics.markReassembled();
          context.headers().add("correlation-id", value.correlationId().getBytes(StandardCharsets.UTF_8));
          context.headers().add("causation-id", value.id().getBytes(StandardCharsets.UTF_8));
          ReassembledEvent reassembled = new ReassembledEvent(value.id(), state.join(), value.correlationId());
          return KeyValue.pair(key, new ChunkMergeResult(reassembled, null));
        }
        return null;
      }

      private void expire(long timestamp) {
        try (var iterator = store.all()) {
          while (iterator.hasNext()) {
            KeyValue<String, ChunkAccumulatorState> entry = iterator.next();
            ChunkAccumulatorState state = entry.value;
            if (timestamp - state.updatedAt() >= timeout.toMillis()) {
              store.delete(entry.key);
              metrics.markTimedOut();
              ChunkTimeout timeoutEvent =
                  new ChunkTimeout(state.id(), state.missingSequences(), state.correlationId());
              context.forward(entry.key, new ChunkMergeResult(null, timeoutEvent));
            }
          }
        }
      }

      @Override
      public void close() {}
    };
  }
}
