package com.fattahpour.kstreamspatterns.eventchunking;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

final class ChunkHeaderTransformer
    implements ValueTransformerWithKeySupplier<String, EventChunk, EventChunk> {
  private final PatternMetrics metrics;

  ChunkHeaderTransformer(PatternMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ValueTransformerWithKey<String, EventChunk, EventChunk> get() {
    return new ValueTransformerWithKey<>() {
      private ProcessorContext<String, EventChunk> context;

      @Override
      public void init(ProcessorContext<String, EventChunk> context) {
        this.context = context;
      }

      @Override
      public EventChunk transform(String readOnlyKey, EventChunk value) {
        metrics.markChunked();
        context.headers().add("correlation-id", value.correlationId().getBytes(StandardCharsets.UTF_8));
        context.headers().add("chunk-seq", Integer.toString(value.sequence()).getBytes(StandardCharsets.UTF_8));
        context.headers().add("chunk-total", Integer.toString(value.totalChunks()).getBytes(StandardCharsets.UTF_8));
        return value;
      }

      @Override
      public void close() {}
    };
  }
}
