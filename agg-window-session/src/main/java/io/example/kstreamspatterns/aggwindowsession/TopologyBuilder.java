package io.example.kstreamspatterns.aggwindowsession;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input");
    String output = System.getProperty("output.topic", "session-count");
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        .toStream()
        .filter((k, v) -> v != null)
        .map(
            (Windowed<String> k, Long v) ->
                KeyValue.pair(
                    k.key() + "@" + k.window().start() + "-" + k.window().end(),
                    v.toString()))
        .to(output, Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
