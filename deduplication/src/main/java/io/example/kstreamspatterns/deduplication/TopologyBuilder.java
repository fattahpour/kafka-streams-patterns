package io.example.kstreamspatterns.deduplication;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-dedup");
    String output = System.getProperty("output.topic", "output-dedup");
    StreamsBuilder builder = new StreamsBuilder();

    KeyValueBytesStoreSupplier storeSupplier =
        Stores.inMemoryKeyValueStore("dedup-store");
    builder.addStateStore(
        Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long()));

    KStream<String, String> source =
        builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()));
    KStream<String, String> deduped =
        source.transformValues(new DeduplicationSupplier(Duration.ofMinutes(10)), "dedup-store");
    deduped.to(output, Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }

  static class DeduplicationSupplier implements ValueTransformerWithKeySupplier<String, String, String> {
    private final Duration window;

    DeduplicationSupplier(Duration window) {
      this.window = window;
    }

    @Override
    public ValueTransformerWithKey<String, String, String> get() {
      return new DeduplicationTransformer(window);
    }
  }

  static class DeduplicationTransformer implements ValueTransformerWithKey<String, String, String> {
    private final Duration window;
    private KeyValueStore<String, Long> store;
    private ProcessorContext context;

    DeduplicationTransformer(Duration window) {
      this.window = window;
    }

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
      this.store = context.getStateStore("dedup-store");
      context.schedule(window, PunctuationType.WALL_CLOCK_TIME, ts -> prune(ts));
    }

    @Override
    public String transform(String readOnlyKey, String value) {
      if (readOnlyKey == null) {
        return null;
      }
      long timestamp = context.timestamp();
      Long lastSeen = store.get(readOnlyKey);
      if (lastSeen == null || timestamp - lastSeen > window.toMillis()) {
        store.put(readOnlyKey, timestamp);
        return value;
      }
      return null;
    }

    private void prune(long timestamp) {
      try (var iter = store.all()) {
        while (iter.hasNext()) {
          KeyValue<String, Long> kv = iter.next();
          if (timestamp - kv.value > window.toMillis()) {
            store.delete(kv.key);
          }
        }
      }
    }

    @Override
    public void close() {}
  }
}
