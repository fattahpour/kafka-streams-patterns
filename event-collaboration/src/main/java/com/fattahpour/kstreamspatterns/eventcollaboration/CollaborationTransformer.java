package com.fattahpour.kstreamspatterns.eventcollaboration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class CollaborationTransformer
    implements TransformerSupplier<String, CollaborationEnvelope, KeyValue<String, CollaborationOutcome>> {
  static final String STORE_NAME = "collaboration-store";
  private final long latenessMs;
  private final PatternMetrics metrics;

  CollaborationTransformer(long latenessMs, PatternMetrics metrics) {
    this.latenessMs = latenessMs;
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, CollaborationEnvelope, KeyValue<String, CollaborationOutcome>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, CollaborationState> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, CollaborationState>) context.getStateStore(STORE_NAME);
      }

      @Override
      public KeyValue<String, CollaborationOutcome> transform(String key, CollaborationEnvelope value) {
        if (key == null || value == null) {
          return null;
        }
        CollaborationState state = store.get(key);
        if (state == null) {
          state = new CollaborationState();
        }
        if (value.correlationId() != null) {
          state.setCorrelationId(value.correlationId());
        }
        long timestamp = context.timestamp();
        if (value.sourceType() == SourceType.ALPHA) {
          metrics.markAlpha();
          state.setAlphaValue(value.value());
          state.setAlphaTimestamp(timestamp);
        } else {
          metrics.markBeta();
          state.setBetaValue(value.value());
          state.setBetaTimestamp(timestamp);
        }
        store.put(key, state);

        Long alphaTs = state.alphaTimestamp();
        Long betaTs = state.betaTimestamp();
        if (alphaTs == null || betaTs == null) {
          return null;
        }
        long diff = Math.abs(alphaTs - betaTs);
        if (diff <= latenessMs) {
          metrics.markJoined();
          CollaboratedEvent joined =
              new CollaboratedEvent(
                  key,
                  state.alphaValue(),
                  state.betaValue(),
                  state.correlationId(),
                  Math.max(alphaTs, betaTs));
          store.put(key, new CollaborationState());
          return KeyValue.pair(key, new CollaborationOutcome(joined, null));
        }
        metrics.markLate();
        LateEvent late = new LateEvent(key, value.sourceType().name(), timestamp);
        if (alphaTs < betaTs) {
          state.setAlphaValue(null);
          state.setAlphaTimestamp(null);
        } else {
          state.setBetaValue(null);
          state.setBetaTimestamp(null);
        }
        store.put(key, state);
        return KeyValue.pair(key, new CollaborationOutcome(null, late));
      }

      @Override
      public void close() {}
    };
  }
}
