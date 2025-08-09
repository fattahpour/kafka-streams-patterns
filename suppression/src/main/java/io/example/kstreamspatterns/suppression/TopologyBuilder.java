package io.example.kstreamspatterns.suppression;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-suppression");
    String output = System.getProperty("output.topic", "output-suppression");
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((Windowed<String> k, Long v) -> KeyValue.pair(k.key(), v))
        .to(output, Produced.with(Serdes.String(), Serdes.Long()));
    return builder.build();
  }
}
