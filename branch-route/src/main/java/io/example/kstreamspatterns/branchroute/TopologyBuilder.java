package io.example.kstreamspatterns.branchroute;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-branch");
    String even = System.getProperty("even.topic", "even-branch");
    String odd = System.getProperty("odd.topic", "odd-branch");
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
    branches[0].to(even);
    branches[1].to(odd);
    return builder.build();
  }
}
