package io.example.kstreamspatterns.materializedviews;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class TopologyBuilder {
  public static Topology build() {
    String inputTopic = System.getProperty("input.topic");
    String outputTopic = System.getProperty("output.topic");

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey()
        .count(Materialized.as("counts-store"))
        .toStream()
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }
}
