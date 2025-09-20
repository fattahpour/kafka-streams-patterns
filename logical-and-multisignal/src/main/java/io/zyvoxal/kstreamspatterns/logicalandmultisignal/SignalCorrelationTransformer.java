package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class SignalCorrelationTransformer implements TransformerSupplier<String, SignalEnvelope, KeyValue<String, CorrelationResult>> {
  static final String STORE_NAME = "multi-signal-store";
  private final Duration window;
  private final PatternMetrics metrics;

  SignalCorrelationTransformer(Duration window, PatternMetrics metrics) {
    this.window = window;
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, SignalEnvelope, KeyValue<String, CorrelationResult>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, CorrelationState> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, CorrelationState>) context.getStateStore(STORE_NAME);
        context.schedule(window, PunctuationType.WALL_CLOCK_TIME, this::expire);
      }

      @Override
      public KeyValue<String, CorrelationResult> transform(String key, SignalEnvelope value) {
        if (key == null || value == null) {
          return null;
        }
        metrics.markProcessed();
        SignalType type;
        try {
          type = SignalType.fromString(value.signalType());
        } catch (IllegalArgumentException e) {
          return null;
        }
        CorrelationState state = store.get(key);
        if (state == null) {
          state = new CorrelationState(key, value.correlationId(), context.timestamp());
        }
        state.add(type, value.payload(), value.correlationId(), context.timestamp());
        state.setCorrelationKey(key);
        store.put(key, state);
        if (state.isComplete()) {
          store.delete(key);
          metrics.markCompleted();
          context.headers().add("correlation-id", state.correlationId().getBytes(StandardCharsets.UTF_8));
          Map<SignalType, String> payloads = new EnumMap<>(SignalType.class);
          state.payloads().forEach((k, v) -> payloads.put(SignalType.valueOf(k), v));
          CorrelatedSignal correlated =
              new CorrelatedSignal(key, state.correlationId(), payloads, context.timestamp());
          return KeyValue.pair(key, new CorrelationResult(correlated, null));
        }
        return null;
      }

      private void expire(long timestamp) {
        try (var iterator = store.all()) {
          while (iterator.hasNext()) {
            KeyValue<String, CorrelationState> entry = iterator.next();
            CorrelationState state = entry.value;
            if (timestamp - state.firstTimestamp() >= window.toMillis()) {
              store.delete(entry.key);
              metrics.markExpired();
              ExpiredCorrelation expired =
                  new ExpiredCorrelation(entry.key, state.correlationId(), state.missingSignals());
              context.forward(entry.key, new CorrelationResult(null, expired));
            }
          }
        }
      }

      @Override
      public void close() {}
    };
  }
}
