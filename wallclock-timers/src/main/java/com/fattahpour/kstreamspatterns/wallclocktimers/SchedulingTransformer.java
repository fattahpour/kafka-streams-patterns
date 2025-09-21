package com.fattahpour.kstreamspatterns.wallclocktimers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class SchedulingTransformer
    implements TransformerSupplier<String, ScheduleCommand, KeyValue<String, TimerFiredEvent>> {
  private final long intervalMs;
  private final long graceMs;
  private final PatternMetrics metrics;

  SchedulingTransformer(long intervalMs, long graceMs, PatternMetrics metrics) {
    this.intervalMs = intervalMs;
    this.graceMs = graceMs;
    this.metrics = metrics;
  }

  @Override
  public Transformer<String, ScheduleCommand, KeyValue<String, TimerFiredEvent>> get() {
    return new Transformer<>() {
      private ProcessorContext context;
      private KeyValueStore<String, ScheduledTimer> store;

      @Override
      @SuppressWarnings("unchecked")
      public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, ScheduledTimer>) context.getStateStore(WallclockTimersTopology.STORE_NAME);
        context.schedule(Duration.ofMillis(intervalMs), PunctuationType.WALL_CLOCK_TIME, this::fireDue);
      }

      @Override
      public KeyValue<String, TimerFiredEvent> transform(String key, ScheduleCommand value) {
        if (key == null || value == null) {
          return null;
        }
        metrics.markScheduled();
        ScheduledTimer timer =
            new ScheduledTimer(value.id(), value.dueAt(), value.payload(), value.correlationId(), context.timestamp());
        store.put(value.id(), timer);
        return null;
      }

      private void fireDue(long timestamp) {
        try (var iterator = store.all()) {
          while (iterator.hasNext()) {
            KeyValue<String, ScheduledTimer> entry = iterator.next();
            ScheduledTimer timer = entry.value;
            if (timer.dueAt() <= timestamp + graceMs) {
              store.delete(entry.key);
              metrics.markFired();
              TimerFiredEvent fired =
                  new TimerFiredEvent(
                      timer.id(), timer.dueAt(), timestamp, timer.payload(), timer.correlationId());
              if (timer.correlationId() != null) {
                context.headers()
                    .add(
                        "correlation-id",
                        timer.correlationId().getBytes(StandardCharsets.UTF_8));
              }
              context.headers()
                  .add("timer-id", timer.id().getBytes(StandardCharsets.UTF_8));
              context.headers()
                  .add(
                      "timer-due-at",
                      Long.toString(timer.dueAt()).getBytes(StandardCharsets.UTF_8));
              context.forward(entry.key, fired);
            }
          }
        }
      }

      @Override
      public void close() {}
    };
  }
}
