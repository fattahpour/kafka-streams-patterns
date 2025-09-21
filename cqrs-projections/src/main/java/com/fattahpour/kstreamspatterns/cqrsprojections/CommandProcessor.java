package com.fattahpour.kstreamspatterns.cqrsprojections;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class CommandProcessor
    implements TransformerSupplier<String, Command, KeyValue<String, ProjectionOutcome>> {
  static final String STORE_NAME = "projection-store";
  private final PatternMetrics metrics;

  CommandProcessor(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, Command, KeyValue<String, ProjectionOutcome>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, ProjectionState> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, ProjectionState>) context.getStateStore(STORE_NAME);
      }

      @Override
      public KeyValue<String, ProjectionOutcome> transform(String key, Command value) {
        if (value == null || value.aggregateId() == null || value.type() == null) {
          metrics.markRejected();
          return KeyValue.pair(key, new ProjectionOutcome(null, new CommandError(key, "invalid-command")));
        }
        CommandType type;
        try {
          type = CommandType.fromString(value.type());
        } catch (IllegalArgumentException e) {
          metrics.markRejected();
          return KeyValue.pair(
              value.aggregateId(), new ProjectionOutcome(null, new CommandError(value.aggregateId(), "unknown-type")));
        }
        ProjectionState state = store.get(value.aggregateId());
        switch (type) {
          case CREATE:
            if (state != null) {
              metrics.markRejected();
              return KeyValue.pair(
                  value.aggregateId(),
                  new ProjectionOutcome(null, new CommandError(value.aggregateId(), "aggregate-exists")));
            }
            state = new ProjectionState(value.aggregateId(), value.payload(), 1L);
            store.put(value.aggregateId(), state);
            metrics.markAccepted();
            ProjectionEvent created =
                new ProjectionEvent(value.aggregateId(), value.payload(), state.version(), "CREATE");
            return KeyValue.pair(value.aggregateId(), new ProjectionOutcome(created, null));
          case UPDATE:
            if (state == null) {
              metrics.markRejected();
              return KeyValue.pair(
                  value.aggregateId(),
                  new ProjectionOutcome(null, new CommandError(value.aggregateId(), "aggregate-missing")));
            }
            state.setVersion(state.version() + 1);
            state.setPayload(value.payload());
            store.put(value.aggregateId(), state);
            metrics.markAccepted();
            ProjectionEvent updated =
                new ProjectionEvent(value.aggregateId(), state.payload(), state.version(), "UPDATE");
            return KeyValue.pair(value.aggregateId(), new ProjectionOutcome(updated, null));
          case DELETE:
            if (state == null) {
              metrics.markRejected();
              return KeyValue.pair(
                  value.aggregateId(),
                  new ProjectionOutcome(null, new CommandError(value.aggregateId(), "aggregate-missing")));
            }
            long version = state.version() + 1;
            store.delete(value.aggregateId());
            metrics.markAccepted();
            ProjectionEvent deleted =
                new ProjectionEvent(value.aggregateId(), null, version, "DELETE");
            return KeyValue.pair(value.aggregateId(), new ProjectionOutcome(deleted, null));
          default:
            metrics.markRejected();
            return KeyValue.pair(
                value.aggregateId(), new ProjectionOutcome(null, new CommandError(value.aggregateId(), "unsupported")));
        }
      }

      @Override
      public void close() {}
    };
  }
}
