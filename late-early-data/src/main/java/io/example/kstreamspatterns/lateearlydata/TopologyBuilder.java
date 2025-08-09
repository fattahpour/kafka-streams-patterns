package io.example.kstreamspatterns.lateearlydata;

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
    String input = System.getProperty("input.topic", "late-early-input");
    String output = System.getProperty("output.topic", "late-early-output");

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(input, Consumed.with(Serdes.String(), Serdes.Long()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(5)))
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        .suppress(
            Suppressed.untilTimeLimit(
                    Duration.ofSeconds(1), Suppressed.BufferConfig.unbounded())
                .emitEarly()
                .emitFinal())
        .toStream()
        .map((Windowed<String> k, Long v) -> KeyValue.pair(k.key() + "@" + k.window().start(), v))
        .to(output, Produced.with(Serdes.String(), Serdes.Long()));
    return builder.build();
  }
}
