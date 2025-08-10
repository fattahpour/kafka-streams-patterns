package com.fattahpour.kstreamspatterns.fanoutfanin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "fanout-input");
    String output = System.getProperty("output.topic", "fanout-output");
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> source =
        builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, String>[] branches =
        source.branch(
            (k, v) -> {
              try {
                return Integer.parseInt(v) % 2 == 0;
              } catch (NumberFormatException e) {
                return false;
              }
            },
            (k, v) -> true);

    KStream<String, String> even =
        branches[0].mapValues(v -> String.valueOf(Integer.parseInt(v) * 2));
    KStream<String, String> odd =
        branches[1].mapValues(
            v -> {
              try {
                return String.valueOf(Integer.parseInt(v) * 3);
              } catch (NumberFormatException e) {
                return v;
              }
            });

    even.merge(odd)
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
