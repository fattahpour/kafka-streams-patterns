package com.fattahpour.kstreamspatterns.eventsplitter;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

final class LineageHeaderTransformer implements ValueTransformerWithKeySupplier<String, ChildEvent, ChildEvent> {
  @Override
  public ValueTransformerWithKey<String, ChildEvent, ChildEvent> get() {
    return new ValueTransformerWithKey<>() {
      private ProcessorContext context;

      @Override
      public void init(ProcessorContext context) {
        this.context = context;
      }

      @Override
      public ChildEvent transform(String readOnlyKey, ChildEvent value) {
        if (value == null) {
          return null;
        }
        context.headers().add("lineage-parent-id", value.parentId().getBytes(StandardCharsets.UTF_8));
        context.headers().add(
            "lineage-child-index",
            Integer.toString(value.index()).getBytes(StandardCharsets.UTF_8));
        if (value.correlationId() != null) {
          context.headers()
              .add("correlation-id", value.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        return value;
      }

      @Override
      public void close() {}
    };
  }
}
