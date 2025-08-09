package io.example.kstreamspatterns.statelesstransforms;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-stateless");
    String output = System.getProperty("output.topic", "output-stateless");
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(input, org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues(String::toUpperCase)
        .filter((k, v) -> v != null && !v.startsWith("IGNORE"))
        .flatMapValues(v -> java.util.Arrays.asList(v.split(" ")))
        .to(output);
    return builder.build();
  }
}
