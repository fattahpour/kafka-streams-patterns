package com.fattahpour.kstreamspatterns.rekeyrepartition;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-rekey");
    String output = System.getProperty("output.topic", "output-rekey");
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
        .selectKey((k, v) -> v.split(":")[0])
        .mapValues(v -> v.split(":")[1])
        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
