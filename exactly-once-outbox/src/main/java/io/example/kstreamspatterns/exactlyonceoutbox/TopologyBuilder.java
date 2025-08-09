package io.example.kstreamspatterns.exactlyonceoutbox;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "orders");
    String processed = System.getProperty("processed.topic", "processed-orders");
    String outbox = System.getProperty("outbox.topic", "orders-outbox");
    StreamsBuilder builder = new StreamsBuilder();
    var stream =
        builder
            .stream(input, org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(String::toUpperCase);
    stream.to(processed, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    stream.to(outbox, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
