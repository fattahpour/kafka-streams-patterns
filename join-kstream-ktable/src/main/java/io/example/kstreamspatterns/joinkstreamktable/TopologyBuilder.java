package io.example.kstreamspatterns.joinkstreamktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String stream = System.getProperty("stream.topic", "stream-join");
    String table = System.getProperty("table.topic", "table-join");
    String output = System.getProperty("output.topic", "joined");
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> streamSource =
        builder.stream(stream, Consumed.with(Serdes.String(), Serdes.String()));
    KTable<String, String> tableSource =
        builder.table(table, Consumed.with(Serdes.String(), Serdes.String()));
    streamSource
        .join(tableSource, (s, t) -> s + ":" + t)
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
